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
use Fcntl qw(:seek);
use File::Temp qw/ tempfile /;#tempdir
use Digest::SHA;
use CONST;
use SqliteCursor;
use CSV_IO;

use constant
{
	TYPE_DIR => (1 << 0),
	TYPE_FILE => (1 << 1),
	TYPE_SYMLINK => (1 << 2)
};

use constant
{
	SCRIPT_CREATE_PATH_NAMES_TABLE => ['CREATE TABLE path_names(name_id integer PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)'],
	SCRIPT_CREATE_SNAPSHOT_DB_BASIS => ['CREATE TABLE path_elements(element_id integer PRIMARY KEY NOT NULL, type_id integer NOT NULL REFERENCES path_types(type_id) ON DELETE CASCADE, name_id integer NOT NULL REFERENCES path_names(name_id) ON DELETE CASCADE)',
'CREATE TABLE element_parents(element_id integer PRIMARY KEY NOT NULL, parent_id integer REFERENCES path_elements(element_id) ON DELETE CASCADE, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',#NULL parent indicates root level
'CREATE TABLE element_hash(element_id integer PRIMARY KEY NOT NULL, hash TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',
'CREATE TABLE element_lastmodified(element_id integer PRIMARY KEY NOT NULL, last_modified TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',
'CREATE TABLE element_size(element_id integer PRIMARY KEY NOT NULL, size integer CHECK(size>=0), FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)',
'CREATE TABLE element_link(element_id integer PRIMARY KEY NOT NULL, target TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)'],
	COMPARABLE_TABLES => [
		'path_elements',
		'element_parents',
		'element_hash',
		'element_lastmodified',
		'element_size',
		'element_link'
	]
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

my $createSnap2Tables = [];
my $renameSnap1Tables = [];

foreach my $line (@{+SCRIPT_CREATE_SNAPSHOT_DB_BASIS})
{
	foreach (@{+COMPARABLE_TABLES})
	{
		$line =~ s/(\Q$_\E)/$1.'2'/e;
	}
	push(@$createSnap2Tables, $line);
}
foreach (@{+COMPARABLE_TABLES})
{
	push(@$renameSnap1Tables, sprintf('ALTER TABLE %s RENAME TO %s2', $_, $_));
}

use constant
{
	SCRIPT_CREATE_SNAPSHOT_2_TABLES => $createSnap2Tables,
	SCRIPT_RENAME_SNAPSHOT_1_TABLES => $renameSnap1Tables
};
$createSnap2Tables = undef;
$renameSnap1Tables = undef;


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
        $rval = TYPE_DIR
    }
    elsif (-f $e)
    {
        $rval = TYPE_FILE
    }
    else
    {
        #die("unknown file type: $e");# comment out to return undef
    }
    
    return $rval;
}

sub compileSQS
{
    state $sthBySQS = {};
    my $ref = $_[0];
    if ($ref->{'cmd'})
    {
        return;
    }
    my $sqs = $ref->{'sqs'};
    my $rval;
    if (($rval = $sthBySQS->{$sqs}))
    {
        $ref->{'sth'} = $rval;
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
    $ref->{'cmd'} = $cmd;
    return $rval;
}

sub get
{
    my ($graceful, $multipleRows, $fetchAll, $spec) = @_;
    my $sthBySQS = compileSQS($spec);
    my $sth = $MY_CURSOR->execute(@_[3..$#_]);
    if (defined($sthBySQS))
    {
        $sthBySQS->{$spec->{'sqs'}} = $spec->{'sth'};
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
        my $ref;
        while ($ref = $sth->fetchrow_arrayref)
        {
            push(@rval, $ref);
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
    if (!scalar(@row))
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
    state $jitSth0 = {'sqs' => 'name_id;path_names;where=name=?'};
    state $jitSth1 = {'cmd' => 'INSERT INTO path_names(name) VALUES (?)'};
    state $jitSth2 = {'cmd' => 'INSERT INTO path_elements(type_id, name_id) VALUES(?,?)'};
    state $jitSth3 = {'cmd' => 'INSERT INTO element_parents(element_id, parent_id) VALUES (?,?)'};
    my ($abs_path, $parent_id, $depth) = @_;
    $depth = $depth || 0;
	my $basename = basename($abs_path);
	my $name_id = getOneOrUndef($jitSth0, $basename);
	if (!defined($name_id))
    {
		$MY_CURSOR->execute($jitSth1, $basename);
		$name_id = $MY_CURSOR->lastrowid;
		die 'Failed to insert a row into path_names' unless defined($name_id);
    }
	my $type_id = getPathType($abs_path);
	$MY_CURSOR->execute($jitSth2, $type_id, $name_id);
	my $eid = $MY_CURSOR->lastrowid;
	die 'Failed to insert a row into path_elements' unless defined($eid);
	if (defined($parent_id))
    {
		$MY_CURSOR->execute($jitSth3, $eid, $parent_id);
    }
	if ($type_id != TYPE_DIR && defined($MY_CSV))
    {
		$MY_CSV->writerow($abs_path);
	}
	return ($eid, $type_id);
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
	state $cmd_createTmpPathsTable = {'cmd' => 'CREATE TEMP TABLE IF NOT EXISTS tmp_paths(element_id integer PRIMARY KEY NOT NULL, path TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE)'};
	state $cmd_insertTmpPath = {'cmd' => 'INSERT INTO tmp_paths VALUES (?,?)'};
	state $sqs_pathForElementID = {'sqs' => 'path;tmp_paths;where=element_id=?'};
	state $sqs_nameForElementID = {'sqs' => 'name;path_elements JOIN path_names on path_names.name_id=path_elements.name_id;where=element_id=?'};
	state $sqs_pidForElementID = {'sqs' => 'parent_id;element_parents;where=element_id=?'};
	
	defined($MY_CURSOR) || die;
	$MY_CURSOR->execute($cmd_createTmpPathsTable);
	
	my ($element_id, $noCaching) = @_;
	
	my $path = getOneOrUndef($sqs_pathForElementID, $element_id);
	if (!defined($path) && !$noCaching)
	{
		# get parent until no parent or has path in cache for element_id
		my $target_id = $element_id;
		my %pathsForElementIDs = ();
		my ($name,$k,$v);
		while (!defined($path))
		{
			$name = getOne($sqs_nameForElementID, $element_id);
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
	state $sqs_elementsInDir = {'sqs' => 'name,type_id,hash,size,target;element_parents JOIN path_elements ON element_parents.element_id=path_elements.element_id JOIN path_names ON path_elements.name_id=path_names.name_id LEFT JOIN element_hash ON element_parents.element_id=element_hash.element_id LEFT JOIN element_size ON element_parents.element_id=element_size.element_id LEFT JOIN element_link ON element_parents.element_id=element_link.element_id;where=parent_id=?;order_by=element_parents.element_id asc'};
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

sub getCommonPathPrefix
{
	state $pathSep = File::Spec->catfile('', '');
	state $pathSepQuoted = quotemeta $pathSep;
	state $e = undef;
	state $getPathHead = sub
	{
		$e = $_[0];
		if ($e =~ /((?<!$pathSepQuoted))$pathSepQuoted/)
		{
			return (sprintf('%s%s', $`, $1), ($' eq '') ? undef : $');
		}
		return ($e, undef);
	};
	state $p1 = undef;
	state $p2 = undef;
	state $ph1 = undef;
	state $ph2 = undef;
	state $pt1 = undef;
	state $pt2 = undef;
	state $step = sub
	{
		($ph1, $pt1) = $getPathHead->($p1);
		if (defined($ph1))
		{
			($ph2, $pt2) = $getPathHead->($p2);
		}
		else
		{
			$ph2 = undef;
		}
	};
	!defined($e) || die;
	($p1, $p2) = @_;
	my @rval = ();
	$step->();
	while (defined($ph2) && $ph2 eq $ph1)
	{
		push(@rval, $ph2);
		last if (!defined($pt1) || !defined($pt2));
		$p1 = $pt1;
		$p2 = $pt2;
		$step->();
	}
	$p1 = undef;
	$p2 = undef;
	$ph1 = undef;
	$ph2 = undef;
	$pt1 = undef;
	$pt2 = undef;
	$e = undef;
	return scalar(@rval) ? join($pathSep, @rval) : undef;
}

sub getInfoForElement
{
	state $newFileCmdList = [
		{'cmd' => 'INSERT INTO element_size(element_id,size) VALUES (?,?)'},
		{'cmd' => 'INSERT INTO element_lastmodified(element_id,last_modified) VALUES (?,?)'},
		{'cmd' => 'INSERT INTO element_hash(element_id,hash) VALUES (?,?)'}
	];
	state $cmd_newSymlink = {'cmd' => 'INSERT INTO element_link(element_id,target) VALUES (?,?)'};
	state $cmd_newDir = {'cmd' => 'INSERT INTO element_hash(element_id,hash) VALUES (?,?)'};
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
			$file_abs_path = getElementPath($element_id);
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
			$file_abs_path = getElementPath($element_id);
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
	state $sqs_numElements = {'sqs' => 'COUNT(*);path_elements'};
	state $sqs_numOfType = {'sqs' => 'COUNT(*);path_elements;where=type_id=?'};
	state $sqs_elementAndType_ascID_notType = {'sqs' => 'element_id,type_id;path_elements;where=type_id<>?;order_by=element_id ASC'};
	state $sqs_element_descID_ofType = {'sqs' => 'element_id;path_elements;where=type_id=?;order_by=element_id DESC'};
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
		}
		print $atMax;
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


__END__
