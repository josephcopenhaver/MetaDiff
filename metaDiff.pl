BEGIN
{
    use Cwd 'abs_path';
    use File::Basename;
    push(@INC, dirname(abs_path($0)));
}
use strict;
use warnings;
use DBI;
use File::Spec;
use feature "state";
use FileHandle;
use File::Basename;
use File::stat;
use File::Copy;
use Fcntl qw(:seek);
use File::Temp qw/ tempfile /;#tempdir
use Digest::SHA;
use CONST;
use SqliteCursor;
use CSV_IO;
use bytes;
use Scalar::Util qw(looks_like_number);

use constant
{
	TYPE_DIR => (1 << 0),
	TYPE_FILE => (1 << 1),
	TYPE_SYMLINK => (1 << 2)
};

sub newSqlCmd
{
	my ($idx, $val) = @_;
	
	if (CONST::IDX_SQS == $idx)
	{
		return [undef, undef, $val];
	}
	if (CONST::IDX_CMD == $idx)
	{
		return [undef, $val];
	}
	
	die;
};

use constant
{
	SCRIPT_CREATE_PATH_NAMES_TABLE => ['CREATE TABLE path_names(name_id integer PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)'],
	SCRIPT_CREATE_SNAPSHOT_DB_BASIS => ['CREATE TABLE path_elements(element_id integer PRIMARY KEY NOT NULL, type_id integer NOT NULL REFERENCES path_types(type_id) ON DELETE CASCADE, name_id integer NOT NULL REFERENCES path_names(name_id) ON DELETE CASCADE)',
'CREATE TABLE element_extensions(element_id integer PRIMARY KEY NOT NULL REFERENCES path_elements(element_id) ON DELETE CASCADE, name_id integer NOT NULL REFERENCES path_names(name_id) ON DELETE CASCADE)',
'CREATE TABLE element_parents(element_id integer PRIMARY KEY NOT NULL, parent_id integer REFERENCES path_elements(element_id) ON DELETE CASCADE, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',#NULL parent indicates root level
'CREATE TABLE element_hash(element_id integer PRIMARY KEY NOT NULL, hash TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',
'CREATE TABLE element_lastmodified(element_id integer PRIMARY KEY NOT NULL, last_modified TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',
'CREATE TABLE element_size(element_id integer PRIMARY KEY NOT NULL, size integer CHECK(size>=0), FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',
'CREATE TABLE element_link(element_id integer PRIMARY KEY NOT NULL, target TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)']
};

use constant
{
	SCRIPT_CREATE_META_INFO_TABLE => ['CREATE TABLE path_types(type_id integer PRIMARY KEY NOT NULL, type_description TEXT UNIQUE)', @{+SCRIPT_CREATE_PATH_NAMES_TABLE}]
};

use constant
{
	SCRIPT_CREATE_SNAPSHOT_DB => [@{+SCRIPT_CREATE_META_INFO_TABLE}, @{+SCRIPT_CREATE_SNAPSHOT_DB_BASIS}],
	SCRIPT_INIT_SNAPSHOT_DB => [sprintf('INSERT INTO path_types SELECT %d AS type_id, \'directory\' AS type_description UNION SELECT %d,\'file\' UNION SELECT %d,\'symlink\'', TYPE_DIR, TYPE_FILE, TYPE_SYMLINK)]
};


###


my $MY_CSV = undef;
my $MY_CURSOR = undef;
my $numArgv = scalar(@ARGV);


if ($numArgv > 0)
{
	$_ = $ARGV[0];
	if ($_ eq 'snapshot')
	{
		$numArgv == 3 || die;
		getDirSnapshot(@ARGV[1..$#ARGV]);
	}
	elsif ($_ eq 'diff')
	{
		$numArgv == 3 || die;
		getSnapshotDiff(@ARGV[1..$#ARGV]);
	}
	else
	{
		die;
	}
}



# TODO: show usage on error/invalid cli operands



exit(0);



sub join_path
{
	return File::Spec->join(@_);
}

sub doF
{
	my ($doF, $doL) = @_;
	my @args = @_[2..$#_];
	eval
	{
		$doF->(@args);
	};
	if ($@)
	{
		my $__em = $@;
		my $__eb = $!;
		$@ = undef;
		$! = undef;
		eval
		{{
			$doL->();
		}};
		if ($@)
		{
			my $e = $!;
			if ($e)
			{
				$e = "\n$e";
			}
			warn "$@$e\n";
		}
		if ($__eb)
		{
			$__eb = "\n$__eb";
		}
		my @traceStep;
		my $i = 1;
		while (scalar(@traceStep = caller($i)))
		{
			warn sprintf("%s:%s in function %s\n", @traceStep[1..$#traceStep]);
			$i++;
		}
		die "$__em$__eb"; # let root error trickle up
	}
	return $doL->();
}

sub getDirContents
{
	state $rval;
	state $refs = [];
	state $doF = sub
	{
		my @files = ();
		my $d;
		my $s = scalar(@$refs);
        my $e;
		push(@$refs, undef);
		opendir($d, $_[0]) || die;
		$refs->[$s] = $d;
		while ($e = readdir($d))
        {
            if ($e !~ /^[.][.]?$/)
            {
                push(@files, $e);
            }
        }
		$rval = \@files;
	};
	state $doL = sub
	{
		my $d = pop(@$refs);
		if (defined($d))
		{
			closedir($d) || die;
		}
	};
	my $r;
	doF($doF, $doL, @_);
	$r = $rval;
	$rval = undef;
	return sort(@$r);
}

sub getPathType
{
    my $e = $_[0];
    my $rval = undef;
    if (-l $e)
    {
        $rval = TYPE_SYMLINK;
    }
    elsif (-d $e)
    {
        $rval = TYPE_DIR;
    }
    elsif (-f $e)
    {
        $rval = TYPE_FILE;
    }
    else
    {
        die("unknown file type: $e");# comment out to return undef #TODO: define how to best handle this case and if possible to reach
    }
    
    return $rval;
}

sub compileSQS
{
    state $sthBySQS = {};
	my $ref = $_[0];
    if ($ref->[CONST::IDX_CMD])
    {
        return;
    }
    my $sqs = $ref->[CONST::IDX_SQS];
    my $rval;
    if (defined($rval = $sthBySQS->{$sqs}))
    {
        $ref->[CONST::IDX_STH] = $rval;
        $rval = undef;
    }
    else
    {
        $rval = $sthBySQS;
    }
    my $fields = undef;
    my $tables = undef;
    my $where = undef;
    my $orderBy = undef;
    my $inNamedSection = 0;
    my $name = undef;
    my $e;
    foreach $e (split(/;/, $sqs))
    {
        if (!$inNamedSection)
        {
            if ($e =~ /^([a-zA-Z_0-9]+)=/)
            {
                $name = $1;
                $e = $';
                $inNamedSection = 1;
            }
            elsif (!defined($fields))
            {
                $fields = $e;
            }
            elsif (!defined($tables))
            {
                $tables = $e;
            }
            else
            {
                die;
            }
        }
        if ($inNamedSection)
        {
            if (!defined($name))
            {
                ($e =~ /^([a-zA-Z_0-9]+)=/) || die;
                $name = $1;
                $e = $';
            }
            if ($name eq 'where')
            {
                !defined($where) || die;
                $where = $e;
            }
            elsif ($name eq 'order_by')
            {
                !defined($orderBy) || die;
                $orderBy = $e;
            }
            else
            {
                die "unknown construct: '$name'";
            }
            
            $name = undef;
        }
    }
    defined($fields) || die;
    defined($tables) || die;
    my $cmd = sprintf('SELECT %s FROM %s%s%s%s%s', $fields, $tables, (defined($where)) ? ' WHERE ' : '', (defined($where)) ? $where : '', (defined($orderBy)) ? ' ORDER BY ' : '', (defined($orderBy)) ? $orderBy : '');
    $ref->[CONST::IDX_CMD] = $cmd;
    return $rval;
}

sub get
{
    my ($graceful, $multipleRows, $fetchAll, $spec) = @_;
    my $sthBySQS = compileSQS($spec);
    my $sth = $MY_CURSOR->execute(@_[3..$#_]);
    if (defined($sthBySQS))
    {
        $sthBySQS->{$spec->[CONST::IDX_SQS]} = $spec->[CONST::IDX_STH];
    }
    my @row = $sth->fetchrow_array();
    my $numColumns = scalar(@row);
    $graceful || $numColumns || die;
    
    if (!$numColumns)
    {
        #assert no error
        die $sth->err if $sth->err;
		if ($multipleRows && $fetchAll)
		{
			return @row;
		}
        return undef;
    }
    
    if ($multipleRows)
    {
        if (!$fetchAll)
        {
            return ($sth, @row);
        }
        my @rval = (\@row);
        my $ref;# $ref appears to be a reused pointer in the supplying module, must deference it!
        while ($ref = $sth->fetchrow_arrayref)
        {
            push(@rval, [@$ref]);
        }
        die $sth->err if $sth->err;
        return @rval;
    }
    else
    {
        $sth->finish;
        die if (!$fetchAll && $numColumns > 1);
        if ($fetchAll)
        {
            return @row;
        }
        return $row[0];
    }
}

sub getOne
{
    return get(0, 0, 0, @_);
}

sub getOneOrUndef
{
   return get(1, 0, 0, @_);
}

sub getRow
{
    return get(0, 0, 1, @_);
}

sub getRowOrUndef
{
    return get(1, 0, 1, @_);
}

sub _doForRows
{
    state $s_inc = sub
    {
        ${$_[0]}++;
    };
    state $s_nop = sub
    {
        # Do Nothing
    };
    my ($min, $max, $cb) = @_;
    die if ($min > $max && $max != -1);
    die if ($min < 0);
    my $inc;
    if ($max < 0)
    {
        $inc = $s_nop;
        $min == 0 || die;
        $max = $min + 1;
    }
    else
    {
        $inc = $s_inc;
    }
    my $i = 0;
    my $_i = \$i;
    
    
    my @row = get(($min <= 0), 1, 0, @_[3..$#_]);
    if (scalar(@row) < 2)
    {
        return;
    }
    
    my $sth = $row[0];
    $cb->(@row[1..$#row]);
    $inc->($_i);
    while ($i < $max)
    {
        @row = $sth->fetchrow_array();
        if (scalar(@row))
        {
            $cb->(@row);
            $inc->($_i);
        }
        else
        {
            die $sth->errstr if $sth->err;
            last;
        }
    }
    die if ($i < $min);
    $sth->finish;
}

sub doForNRows
{
    my $n = $_[0];
    die if ($n < 0);
    return if ($n < 1);
    return _doForRows($n, @_);
}

sub doForMaxNRows
{
    my $n = $_[0];
    die if ($n < 0);
    return if ($n < 1);
    return _doForRows(0, @_);
}

sub doForAllRows
{
    return _doForRows(0, -1, @_);
}

sub addElement
{
    state $sqs_getIDForName = newSqlCmd(CONST::IDX_SQS, 'name_id;path_names;where=name=?');
    state $cmd_addPathName = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO path_names(name) VALUES (?)');
    state $cmd_linkName = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO path_elements(type_id, name_id) VALUES(?,?)');
    state $cmd_linkParent = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_parents(element_id, parent_id) VALUES (?,?)');
	state $cmd_linkExtension = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_extensions(element_id, name_id) VALUES(?,?)');
    my ($abs_path, $parent_id, $depth) = @_;
    $depth = $depth || 0;
	my $type_id = getPathType($abs_path);
	my $basename = basename($abs_path);
	my $name_id;
	my $ext_id = undef;
	if ($type_id == TYPE_FILE && $basename =~ /(?<![\.])\.([^\.]+)$/ && $` ne '')
	{
		$basename = $`;
		$name_id = $1;
		$ext_id = getOneOrUndef($sqs_getIDForName, $name_id);
		if (!defined($ext_id))
		{
			$MY_CURSOR->execute($cmd_addPathName, $name_id);
			$ext_id = $MY_CURSOR->lastrowid;
			die 'Failed to insert a row into path_names' unless defined($ext_id);
		}
		die unless defined($ext_id);
	}
	$name_id = getOneOrUndef($sqs_getIDForName, $basename);
	if (!defined($name_id))
    {
		$MY_CURSOR->execute($cmd_addPathName, $basename);
		$name_id = $MY_CURSOR->lastrowid;
		die 'Failed to insert a row into path_names' unless defined($name_id);
    }
	$MY_CURSOR->execute($cmd_linkName, $type_id, $name_id);
	my $element_id = $MY_CURSOR->lastrowid;
	die 'Failed to insert a row into path_elements' unless defined($element_id);
	if (defined($ext_id))
	{
		$MY_CURSOR->execute($cmd_linkExtension, $element_id, $ext_id);
		die unless defined($MY_CURSOR->lastrowid);
	}
	if (defined($parent_id))
    {
		$MY_CURSOR->execute($cmd_linkParent, $element_id, $parent_id);
    }
	if ($type_id != TYPE_DIR && defined($MY_CSV))
    {
		$MY_CSV->writerow($abs_path);
	}
	return ($element_id, $type_id);
}

sub recurseAddElement
{
	my ($abs_path, $parent_id, $depth) = @_;
    my $e;
    $depth = $depth || 0;
	my ($eid, $type) = addElement($abs_path, $parent_id, $depth);
	if ($type == TYPE_DIR)
    {
		foreach $e (getDirContents($abs_path))
        {
			recurseAddElement(join_path($abs_path, $e), $eid, $depth + 1)
		}
	}
}

sub getElementPath
{
	state $cmd_createTmpPathsTable = newSqlCmd(CONST::IDX_CMD, 'CREATE TEMP TABLE IF NOT EXISTS tmp_paths(element_id integer PRIMARY KEY NOT NULL, path TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)');
	state $cmd_insertTmpPath = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO tmp_paths VALUES (?,?)');
	state $sqs_pathForElementID = newSqlCmd(CONST::IDX_SQS, 'path;tmp_paths;where=element_id=?');
	state $sqs_nameForNonFileID = newSqlCmd(CONST::IDX_SQS, 'name;path_elements JOIN path_names ON path_names.name_id=path_elements.name_id;where=element_id=?');
	state $sqs_nameForFileID = newSqlCmd(CONST::IDX_SQS, 'path_names.name,path_names2.name;path_elements JOIN path_names ON path_names.name_id=path_elements.name_id LEFT JOIN element_extensions ON element_extensions.element_id=path_elements.element_id LEFT JOIN path_names AS path_names2 ON path_names2.name_id=element_extensions.name_id;where=path_elements.element_id=?');
	state $sqs_pidForElementID = newSqlCmd(CONST::IDX_SQS, 'parent_id;element_parents;where=element_id=?');
	
	defined($MY_CURSOR) || die;
	$MY_CURSOR->execute($cmd_createTmpPathsTable);
	
	my ($element_id, $type_id, $noCaching) = @_;
	defined($type_id) || die;
	
	my $path = getOneOrUndef($sqs_pathForElementID, $element_id);
	if (!defined($path) && !$noCaching)
	{
		# get parent until no parent or has path in cache for element_id
		my $target_id = $element_id;
		my %pathsForElementIDs = ();
		my ($name,$ext_name,$k,$v);
		while (!defined($path))
		{
			if ($type_id == TYPE_DIR || $type_id == TYPE_SYMLINK)
			{
				$name = getOne($sqs_nameForNonFileID, $element_id);
			}
			elsif ($type_id == TYPE_FILE)
			{
				($name, $ext_name) = getRow($sqs_nameForFileID, $element_id);
				if (defined($ext_name))
				{
					$name = sprintf("%s.%s", $name, $ext_name);
					$ext_name = undef;
				}
				$type_id = TYPE_DIR;
			}
			else
			{
				die;
			}
			
			while (($k, $v) = each %pathsForElementIDs)
			{
				$pathsForElementIDs{$k} = join_path($name, $v);
			}
			$pathsForElementIDs{$element_id} = $name;
			$element_id = getOneOrUndef($sqs_pidForElementID, $element_id);
			last if !defined($element_id);
			$path = getOneOrUndef($sqs_pathForElementID, $element_id);
		}
		if (defined($path))
		{
			while (($k, $v) = each %pathsForElementIDs)
			{
				$pathsForElementIDs{$k} = join_path($path, $v);
			}
		}
		while (($k, $v) = each %pathsForElementIDs)
		{
			$MY_CURSOR->execute($cmd_insertTmpPath, $k, $v);
		}
		$path = $pathsForElementIDs{$target_id};
	}
	defined($path) || die;
	return $path;
}

sub getFileSize
{
	my $rval = -s $_[0];
	return $rval;
}

sub getMTime
{
	my $rval = stat($_[0])->mtime;
	return $rval;
}

sub getHashObj
{
	state $hashLength = 256;
	
	state $rval = new Digest::SHA->new($hashLength); # TODO: nothing that depends on this function can run in parallel
	$rval->reset();
	
	return $rval;
}

sub getFileHash
{
	my $hashObj = getHashObj();
	my $rval;
	$hashObj->addfile($_[0]);
	$rval = $hashObj->b64digest();
	
	return $rval;
}

sub getDirHash
{
	#element_parents.element_id, -- not part of query because not used
	# only useful for debugging
	state $sqs_elementsInDir = newSqlCmd(CONST::IDX_SQS, 'name,type_id,hash,size,target;element_parents JOIN path_elements ON element_parents.element_id=path_elements.element_id JOIN path_names ON path_elements.name_id=path_names.name_id LEFT JOIN element_hash ON element_parents.element_id=element_hash.element_id LEFT JOIN element_size ON element_parents.element_id=element_size.element_id LEFT JOIN element_link ON element_parents.element_id=element_link.element_id;where=parent_id=?;order_by=element_parents.element_id ASC');
	state $hashObj = undef;
	state $name = undef;
	state $type_id = undef;
	state $hash = undef;
	state $size = undef;
	state $target = undef;
	state $doForRow = sub
	{
		($name, $type_id, $hash, $size, $target) = @_;
		$hashObj->add($name);
		if ($type_id == TYPE_DIR)
		{
			$hashObj->add($hash);
		}
		elsif ($type_id == TYPE_FILE)
		{
			$hashObj->add($hash, $size);
		}
		elsif ($type_id == TYPE_SYMLINK)
		{
			$hashObj->add($target);
		}
		else
		{
			die;
		}
	};
	#
	!defined($hashObj) || die;
	my $hashObj2;
	my $rval;
	$hashObj = getHashObj();
	
	doForAllRows($doForRow, $sqs_elementsInDir, @_);
	
	$hashObj2 = $hashObj;
	$hashObj = undef;
	$rval = $hashObj2->b64digest();
	
	return $rval;
}

sub getCommonPathLength
{
	state $ps = File::Spec->catfile('', '');
	my ($p1, $p2, $l, $ub) = @_;
	
	if ((!defined($l)) || (!defined($ub)))
	{
		($l, $ub) = (length($p1), length($p2));
	}
	if (($l == $ub) && ($p1 eq $p2))
	{
		return $l;
	}
	
	if ($l < $ub)
	{
		$ub = $l;
	}
	
	my ($si, $i, $s) = (0, 0);
	while (($i < $ub) && (($s = substr($p1, $i, 1)) eq substr($p2, $i, 1)))
	{
		if ($s eq $ps)
		{
			$si = $i;
		}
		$i++;
	}
	
	return $si;
}

sub getCommonPathPrefix
{
	my $r = getCommonPathLength(@_);
	$r = $r ? substr($_[0], 0, $r) : undef;
	return $r;
}

sub getInfoForElement
{
	state $newFileCmdList = [
		newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_size(element_id,size) VALUES (?,?)'),
		newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_lastmodified(element_id,last_modified) VALUES (?,?)'),
		newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_hash(element_id,hash) VALUES (?,?)')
	];
	state $cmd_newSymlink = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_link(element_id,target) VALUES (?,?)');
	state $cmd_newDir = newSqlCmd(CONST::IDX_CMD, 'INSERT INTO element_hash(element_id,hash) VALUES (?,?)');
	#
	defined($MY_CURSOR) || die;
	my ($element_id, $type_id, $abs_src_root_path, $abs_path) = @_;
	defined($element_id) || die;
	defined($type_id) || die;
	defined($abs_src_root_path) == defined($abs_path) || die
	my ($file_abs_path, $hash);
	if ($type_id == TYPE_FILE)
	{
		if (defined($abs_path))
		{
			$file_abs_path = $abs_path;
		}
		else
		{
			$file_abs_path = getElementPath($element_id, $type_id);
			$file_abs_path = join_path($abs_src_root_path, $file_abs_path);
		}
		my $size = getFileSize($file_abs_path);
		my $mtime = getMTime($file_abs_path);
		$hash = getFileHash($file_abs_path);
		my @args = ($size, $mtime, $hash);
		my $cmd;
		my $i = 0;
		foreach $cmd (@$newFileCmdList)
		{
			$MY_CURSOR->execute($cmd, $element_id, $args[$i]);
			$i++;
		}
	}
	elsif ($type_id == TYPE_SYMLINK)
	{
		if (defined($abs_path))
		{
			$file_abs_path = $abs_path;
		}
		else
		{
			$file_abs_path = getElementPath($element_id, $type_id);
			$file_abs_path = join_path($abs_src_root_path, $file_abs_path);
		}
		$file_abs_path = abs_path($file_abs_path);
		my $relpath = $file_abs_path;
		if (defined($abs_src_root_path))
		{
			my $commonPathPrefix = getCommonPathPrefix($abs_src_root_path, $file_abs_path);
			if (defined($commonPathPrefix) && $commonPathPrefix ne '' && $commonPathPrefix eq $abs_src_root_path)
			{
				$relpath = sprintf('.%s', substr($file_abs_path, length($commonPathPrefix)));
			}
		}
		$MY_CURSOR->execute($cmd_newSymlink, $element_id, $relpath);
	}
	elsif ($type_id == TYPE_DIR)
	{
		$hash = getDirHash($element_id);
		$MY_CURSOR->execute($cmd_newDir, $element_id, $hash);
	}
	else
	{
		die
	}
}

sub gatherElementInfo
{
	state $fmt_progress = "%3d.%03d%%\r";
	state $atMin = sprintf($fmt_progress, 0, 0);
	state $atMax = sprintf(substr($fmt_progress, 0, -1) . "\n", 100, 0);
	state $sqs_numElements = newSqlCmd(CONST::IDX_SQS, 'COUNT(*);path_elements');
	state $sqs_numOfType = newSqlCmd(CONST::IDX_SQS, 'COUNT(*);path_elements;where=type_id=?');
	state $sqs_elementAndType_ascID_notType = newSqlCmd(CONST::IDX_SQS, 'element_id,type_id;path_elements;where=type_id<>?;order_by=element_id ASC');
	state $sqs_element_descID_ofType = newSqlCmd(CONST::IDX_SQS, 'element_id;path_elements;where=type_id=?;order_by=element_id DESC');
	state $ioStatePre = undef;
	state $old_fh = undef;
	state $doF = sub
	{
		my $abs_src_root_path = $_[0];
		my $numElements = getOne($sqs_numElements);
		my $numDirs = getOne($sqs_numOfType, TYPE_DIR);
		my $numNonDirs = $numElements - $numDirs;
		
		my $eNum = 0;
		my $lastMsg;
		my $newMsg;
		my $rowRef;
		my $element_id;
		
		if ($numElements <= 0)
		{
			print "Nothing to analyze\n";
			return;
		}
		
		print "Analyzing non-directories\n";
		if ($numNonDirs > 0)
		{
			print $atMin;
			$lastMsg = $atMin;
			$eNum = 0;
			my $type_id;
			my $abs_path;
			foreach $rowRef (get(1, 1, 1, $sqs_elementAndType_ascID_notType, TYPE_DIR))
			{
				$newMsg = sprintf($fmt_progress, $eNum*100/$numNonDirs, ($eNum*100000/$numNonDirs)%1000);
				if ($lastMsg ne $newMsg)
				{
					print $newMsg;
					$lastMsg = $newMsg;
				}
				$eNum++;
				($element_id, $type_id) = @$rowRef;
				($abs_path) = $MY_CSV->readrow();
				defined($abs_path) || die;
				getInfoForElement($element_id, $type_id, $abs_src_root_path, $abs_path);
			}
			print $atMax;
		}
		else
		{
			print "None found\n";
		}
		print "Analyzing directories\n";
		if ($numDirs > 0)
		{
			print $atMin;
			$lastMsg = $atMin;
			$eNum = 0;
			foreach $rowRef (get(1, 1, 1, $sqs_element_descID_ofType, TYPE_DIR))
			{
				$newMsg = sprintf($fmt_progress, $eNum*100/$numDirs, ($eNum*100000/$numDirs)%1000);
				if ($lastMsg ne $newMsg)
				{
					print $newMsg;
					$lastMsg = $newMsg;
				}
				$eNum++;
				($element_id) = @$rowRef;
				getInfoForElement($element_id, TYPE_DIR);
			}
			print $atMax;
		}
		else
		{
			print "None found\n";
		}
	};
	state $doL = sub
	{
		$| = $ioStatePre;
		$ioStatePre = undef;
		select($old_fh);
		$old_fh = undef;
	};
	# end static's
	defined($MY_CURSOR) || die;
	(defined($MY_CSV) && defined($MY_CSV->{'fh'}) && $MY_CSV->{'fm'} eq '<') || die;
	
	# not re-entrant
	!defined($old_fh) || die;
	!defined($ioStatePre) || die;
	
	$old_fh = select(STDOUT);
	$ioStatePre = $|;
	$| = 1;
	return doF($doF, $doL, @_);
}

sub runScript
{
    my ($dbh, $script) = @_;
    my @doArgs = @_[2..$#_];
    my $e;
    foreach $e (@$script)
    {
        warn "E_DO: $e\n" if CONST::DEBUG;
        $dbh->do($e, @doArgs) || die;
    }
}

sub initDB
{
	warn "Initializing new DB\n" if CONST::DEBUG;
	my $dbh = $_[0];
    runScript($dbh, SCRIPT_CREATE_SNAPSHOT_DB);
    runScript($dbh, SCRIPT_INIT_SNAPSHOT_DB);
}

sub newRamDB
{
	my $dbh = DBI->connect(
		'dbi:SQLite:dbname=:memory:', # DSN
		'', # user
		'', # password
		{ RaiseError => 1, AutoCommit => 0 } # other
	) || die;
	
	initDB($dbh);
	
	return $dbh;
}

sub getDirSnapshot
{
	state $callDepth = 0;
	state $dbh = undef;
	state $csvInOutFH = undef;
	state $csvInOut = undef;
	state $out_fh = undef;
	state $cleanupCsv = sub
	{
		if (!defined($csvInOutFH))
		{
			return;
		}
        warn "Cleaning up tmp csv file\n" if CONST::DEBUG;
		my $t = $csvInOut;
		warn "File: '$t'\n" if CONST::DEBUG2;
        $csvInOutFH->close();
		$csvInOutFH = undef;
		$csvInOut = undef;
		unlink($t) || die;
		warn "tmp csv file '$t' cleaned\n" if CONST::DEBUG;
	};
	state $doL = sub
	{
		$callDepth--;
		$cleanupCsv->();
		if (!defined($dbh))
		{
			return;
		}
		$dbh->disconnect();
		$dbh = undef;
	};
	state $doF = sub
	{
		$callDepth++;
		my ($rel_src_path, $out_file_path) = @_;
        my $e;
		my $abs_src_path = File::Spec->rel2abs($rel_src_path);
		my $abs_src_root_path = $abs_src_path;
		($csvInOutFH, $csvInOut) = tempfile(SUFFIX => '_jcope_mdif.csv', UNLINK => 1);
		binmode($csvInOutFH) || die;
        $MY_CSV = CSV_IO->new('>', undef, fh => $csvInOutFH);
		$dbh = newRamDB();
        $MY_CURSOR = SqliteCursor->new($dbh);
		print("Scanning directory structure: $rel_src_path\n");
		if (getPathType($abs_src_path) != TYPE_DIR)
		{
			addElement($abs_src_path);
			$abs_src_root_path = dirname($abs_src_path);
		}
		else
		{
			foreach $e (getDirContents($abs_src_path))
			{
                recurseAddElement(join_path($abs_src_path, $e));
			}
		}
		seek($csvInOutFH, SEEK_SET, 0) || die;
        $MY_CSV = CSV_IO->new('<', undef, fh => $csvInOutFH);
		gatherElementInfo($abs_src_root_path);
		$cleanupCsv->();
		print('Saving data to file... ');
        SqliteCursor->destroyAll();
		$dbh->commit() || die;
		$dbh->sqlite_backup_to_file($out_file_path) || die;
		print("Done\n");
	};
	
	# not re-entrant
	$callDepth == 0 || die;
	
	return doF($doF, $doL, @_);
}

sub strcmp_b
{
	my ($a, $b, $i, $t) = @_;
	my $r;
	
	do
	{
		$r = (ord(substr($a, $i, 1)) <=> ord(substr($b, $i, 1)));
		$i++;
	} while($i < $t && $r == 0);
	
	return $r;
}

sub printChange
{
	printf("%s: %s\n", @_);
}

sub getSnapshotDiff
{
	state $callDepth = 0;
	state $tmpFH1 = undef;
	state $tmpFile1 = undef;
	state $tmpFH2 = undef;
	state $tmpFile2 = undef;
	state $doL = sub
	{
		$callDepth--;
		my ($fh1, $fpath1, $fh2, $fpath2) = ($tmpFH1, $tmpFile1, $tmpFH2, $tmpFile2);
		$tmpFH1 = undef;
		$tmpFile1 = undef;
		$tmpFH2 = undef;
		$tmpFile2 = undef;
		doF(sub{
			if (defined($fh1))
			{
				doF(sub{
					close($fh1) || die;
				},sub{
					unlink($fpath1) || die;
				});
			}
		},sub{
			if (defined($fh2))
			{
				doF(sub{
					close($fh2) || die;
				},sub{
					unlink($fpath2) || die;
				});
			}
		});
	};
	state $doF = sub
	{
		state $dbiHeaderFmt = 'DBI:SQLite:dbname=%s';
		state $sqs_getPathElementCount = newSqlCmd(CONST::IDX_SQS, 'COUNT(*);path_elements');
		state $sqs_getTopPathElementCount = newSqlCmd(CONST::IDX_SQS, 'COUNT(*);path_elements;where=element_id>=?');
		state $sqs_getMatchPathElementCount = newSqlCmd(CONST::IDX_SQS, 'COUNT(*);path_elements;where=element_id=?');
		state $sqs_getAllElements = newSqlCmd(CONST::IDX_SQS, 'path_elements.element_id,type_id,hash,size,last_modified,target;path_elements LEFT JOIN element_lastmodified ON path_elements.element_id=element_lastmodified.element_id LEFT JOIN element_hash ON path_elements.element_id=element_hash.element_id LEFT JOIN element_size ON path_elements.element_id=element_size.element_id LEFT JOIN element_link ON path_elements.element_id=element_link.element_id;order_by=path_elements.element_id ASC');
		$callDepth++;
		my ($dbPath1, $dbPath2, $srcFH) = @_;

		$srcFH = FileHandle->new($dbPath1, '<') || die;
		binmode($srcFH) || die;

		($tmpFH1, $tmpFile1) = tempfile(SUFFIX => '_jcope_mdif.1.snap', UNLINK => 1);
		$tmpFH1 || die;
		binmode($tmpFH1) || die;
		copy($srcFH, $tmpFH1) || die;
		$srcFH->close() || die;
		$tmpFH1->flush() || die;

		$srcFH = FileHandle->new($dbPath2, '<') || die;
		binmode($srcFH) || die;

		($tmpFH2, $tmpFile2) = tempfile(SUFFIX => '_jcope_mdif.2.snap', UNLINK => 1);
		$tmpFH2 || die;
		binmode($tmpFH2) || die;
		copy($srcFH, $tmpFH2) || die;
		$srcFH->close() || die;
		$tmpFH2->flush() || die;
		
		my $dbh1 = sprintf($dbiHeaderFmt, $dbPath1);
		my $dbh1ReadOnly = DBI->connect($dbh1, '', '', { RaiseError => 1, AutoCommit => 0 }) or die $DBI::errstr;
		$dbh1ReadOnly = SqliteCursor->new($dbh1ReadOnly);
		$dbh1 = DBI->connect(sprintf($dbiHeaderFmt, $tmpFile1), '', '', { RaiseError => 1, AutoCommit => 0 }) or die $DBI::errstr;
		$dbh1 = SqliteCursor->new($dbh1);
		
		$MY_CURSOR = $dbh1;
		
		my $count = getOne($sqs_getPathElementCount);
		getOne($sqs_getTopPathElementCount, $count) == 1 || die;
		getOne($sqs_getMatchPathElementCount, $count) == 1 || die;
		$MY_CURSOR = $dbh1ReadOnly;
		getOne($sqs_getPathElementCount) == $count || die;
		getOne($sqs_getTopPathElementCount, $count) == 1 || die;
		getOne($sqs_getMatchPathElementCount, $count) == 1 || die;
		
		my $dbh2 = sprintf($dbiHeaderFmt, $dbPath2);
		my $dbh2ReadOnly = DBI->connect($dbh2, '', '', { RaiseError => 1, AutoCommit => 0 }) or die $DBI::errstr;
		$dbh2ReadOnly = SqliteCursor->new($dbh2ReadOnly);
		$dbh2 = DBI->connect(sprintf($dbiHeaderFmt, $tmpFile2), '', '', { RaiseError => 1, AutoCommit => 0 }) or die $DBI::errstr;
		$dbh2 = SqliteCursor->new($dbh2);
		
		$MY_CURSOR = $dbh2;
		
		$count = getOne($sqs_getPathElementCount);
		getOne($sqs_getTopPathElementCount, $count) == 1 || die;
		getOne($sqs_getMatchPathElementCount, $count) == 1 || die;
		$MY_CURSOR = $dbh2ReadOnly;
		getOne($sqs_getPathElementCount) == $count || die;
		getOne($sqs_getTopPathElementCount, $count) == 1 || die;
		getOne($sqs_getMatchPathElementCount, $count) == 1 || die;
		
		my @row2 = get(1, 1, 0, $sqs_getAllElements);
		$MY_CURSOR = $dbh1ReadOnly;
		my @row1 = get(1, 1, 0, $sqs_getAllElements);
		
		
		my $sth1 = undef;
		my $sth2 = undef;
		if (defined($row1[0]))
		{
			$sth1 = shift(@row1);
		}
		if (defined($row2[0]))
		{
			$sth2 = shift(@row2);
		}
		
		
		print "BEGIN DIFF\n";
		
		
		$MY_CURSOR = undef;# may only be set to dbh1 or dbh2 from here on out
		if (scalar(@row1) && scalar(@row2))
		{
			my $path1 = undef;
			my $path2 = undef;
			my $row1Type;
			my $l1;
			my $l2;
			my $done = 0;
			my $cpl;
			my $cmp;
			my $same;
			my $i;
			my $i1;
			my $i2;
			my $ub = scalar(@row1);
			do
			{
				if (!defined($path1))
				{
					$MY_CURSOR = $dbh1;
					$path1 = getElementPath(@row1[0..1]);
					$l1 = length($path1);
					#print "1: " . $path1 . "\n";
				}
				if (!defined($path2))
				{
					$MY_CURSOR = $dbh2;
					$path2 = getElementPath(@row2[0..1]);
					$l2 = length($path2);
					#print "2: " . $path2 . "\n";
				}
				
				
				$cpl = getCommonPathLength($path1, $path2, $l1, $l2);
				
				
				if (($l1 == $l2) && ($cpl == $l1))
				{
					# same element name
					if (($row1Type = $row1[1]) == $row2[1])
					{
						# types same, could be match or mod
						do
						{{
							$same = 1;
							if ($row1Type == TYPE_DIR)
							{
								# Not doing top level search for differences.
								# Going deep, so do not compare meta info by dir
								last;
							}
							for ($i=2;$i<$ub;$i++)
							{
								$i1 = $row1[$i];
								$i2 = $row2[$i];
								if ((defined($i1) != defined($i2))
									|| (defined($i1)
										&& defined($i2)
										#&& (($i1 <=> $i2) != 0))
										#&& ($i1 ne $i2))
										&& (looks_like_number($i1) ? (($i1 <=> $i2) != 0) : ($i1 ne $i2)))
									)
								{
									$same = 0;
									last;
								}
							}
						}} while (0);
						if (!$same)
						{
							printChange("MOD", $path1);
						}
					}
					else
					{
						printChange("DEL", $path1);
						printChange("ADD", $path2);
					}
					$path1 = undef;
					$path2 = undef;
				}
				elsif ($cpl == $l1)
				{
					# path2 is an entry in directory named $path1
					# should not be reachable
					die;
				}
				elsif ($cpl == $l2)
				{
					# path1 is an entry in a directory named $path2
					# should not be reachable
					die;
				}
				else
				{
					$cmp = strcmp_b($path1, $path2, (($cpl == 0) ? 0 : ($cpl+1)), (($l1 <= $l2) ? $l1 : $l2));
					if ($cmp == 0)
					{
						if ($l1 == $l2)
						{
							die;
						}
						else
						{
							$cmp = ($l1 < $l2) ? (-1) : 1;
						}
					}
					if ($cmp < 0)
					{
						printChange("DEL", $path1);
						$path1 = undef;
					}
					else
					{
						printChange("ADD", $path2);
						$path2 = undef;
					}
				}
				
				
				if (!defined($path1))
				{
					@row1 = $sth1->fetchrow_array();
					$done = (scalar(@row1) == 0);
				}
				if (!defined($path2))
				{
					@row2 = $sth2->fetchrow_array();
					$done = $done || (scalar(@row2) == 0);
				}
				
				
			} while (!$done);
		}
		
		
		if (scalar(@row1))
		{
			# tail end of row1
			$MY_CURSOR = $dbh1;
			do
			{
				printChange("DEL", getElementPath(@row1[0..1]));
				@row1 = $sth1->fetchrow_array();
			} while (scalar(@row1));
		}
		elsif (scalar(@row2))
		{
			# tail end of row2
			$MY_CURSOR = $dbh2;
			do
			{
				printChange("ADD", getElementPath(@row2[0..1]));
				@row2 = $sth2->fetchrow_array();
			} while (scalar(@row2));
		}
		
		
		print "END DIFF\n";
		
		
	};
	
	# not re-entrant
	$callDepth == 0 || die;
	
	return doF($doF, $doL, @_);
}


__END__
