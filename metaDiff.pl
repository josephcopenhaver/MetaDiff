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
use Fcntl qw(:seek);
use File::Temp qw/ tempfile /;#tempdir
use CONST;
use SqliteCursor;

use constant
{
	TYPE_DIR => (1 << 0),
	TYPE_FILE => (1 << 1),
	TYPE_SYMLINK => (1 << 2)
};

use constant
{
	SCRIPT_CREATE_PATH_NAMES_TABLE => ['CREATE TABLE path_names(name_id integer PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE);'],
	SCRIPT_CREATE_SNAPSHOT_DB_BASIS => ['CREATE TABLE path_elements(element_id integer PRIMARY KEY NOT NULL, type_id integer NOT NULL REFERENCES path_types(type_id) ON DELETE CASCADE, name_id integer NOT NULL REFERENCES path_names(name_id) ON DELETE CASCADE);',
'CREATE TABLE element_parents(element_id integer PRIMARY KEY NOT NULL, parent_id integer REFERENCES path_elements(element_id) ON DELETE CASCADE, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE); --NULL parent indicates root level',
'CREATE TABLE element_hash(element_id integer PRIMARY KEY NOT NULL, hash TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);',
'CREATE TABLE element_lastmodified(element_id integer PRIMARY KEY NOT NULL, last_modified TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);',
'CREATE TABLE element_size(element_id integer PRIMARY KEY NOT NULL, size integer CHECK(size>=0), FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);',
'CREATE TABLE element_link(element_id integer PRIMARY KEY NOT NULL, target TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);'],
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
	SCRIPT_CREATE_META_INFO_TABLE => ['CREATE TABLE path_types(type_id integer PRIMARY KEY NOT NULL, type_description TEXT UNIQUE);', @{+SCRIPT_CREATE_PATH_NAMES_TABLE}]
};

use constant
{
	SCRIPT_CREATE_SNAPSHOT_DB => [@{+SCRIPT_CREATE_META_INFO_TABLE}, @{+SCRIPT_CREATE_SNAPSHOT_DB_BASIS}],
	SCRIPT_INIT_SNAPSHOT_DB => [sprintf('INSERT INTO path_types SELECT %d AS type_id, \'directory\' AS type_description UNION SELECT %d,\'file\' UNION SELECT %d,\'symlink\';', TYPE_DIR, TYPE_FILE, TYPE_SYMLINK)]
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
	push(@$renameSnap1Tables, sprintf("ALTER TABLE %s RENAME TO %s2;", $_, $_));
}

use constant
{
	SCRIPT_CREATE_SNAPSHOT_2_TABLES => $createSnap2Tables,
	SCRIPT_RENAME_SNAPSHOT_1_TABLES => $renameSnap1Tables
};
$createSnap2Tables = undef;
$renameSnap1Tables = undef;


###


my $MY_CSV_FD = undef;
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
			warn("$@$e\n");
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
        # Do Nothing
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
    my $cmd = sprintf("SELECT %s FROM %s%s%s%s%s", $fields, $tables, (defined($where)) ? " WHERE " : "", (defined($where)) ? $where : "", (defined($orderBy)) ? " ORDER BY " : "", (defined($orderBy)) ? $orderBy : "");
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
		die "Failed to insert a row into path_names" unless defined($name_id);
    }
	my $type_id = getPathType($abs_path);
	$MY_CURSOR->execute($jitSth2, $type_id, $name_id);
	my $eid = $MY_CURSOR->lastrowid;
	die "Failed to insert a row into path_elements" unless defined($eid);
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

sub gatherElementInfo
{
	#TODO: complete
}

sub runScript
{
    my ($dbh, $script) = @_;
    my @doArgs = @_[2..$#_];
    my $e;
    foreach $e (@$script)
    {
        printf("E_DO: %s\n", $e) if CONST::DEBUG;
        $dbh->do($e, @doArgs) || die;
    }
}

sub initDB
{
	print("Initializing new DB\n") if CONST::DEBUG;
	my $dbh = $_[0];
    runScript($dbh, SCRIPT_CREATE_SNAPSHOT_DB);
    runScript($dbh, SCRIPT_INIT_SNAPSHOT_DB);
}

sub newRamDB
{
	my $dbh = DBI->connect(
		"dbi:SQLite:dbname=:memory:", # DSN
		"", # user
		"", # password
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
		warn("Cleaning up tmp csv file\n") if CONST::DEBUG;
		my $t = $csvInOut;
		warn("File: '$t'\n") if CONST::DEBUG2;
		$csvInOutFH->close();
		$csvInOutFH = undef;
		$csvInOut = undef;
		unlink($t) || die;
		warn("tmp csv file '$t' cleaned\n") if CONST::DEBUG;
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
		($csvInOutFH, $csvInOut) = tempfile(SUFFIX => "_jcope_mdif.csv", UNLINK => 1);
		binmode($csvInOutFH) || die;
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
		gatherElementInfo($abs_src_root_path);
		$cleanupCsv->();
		print("Saving data to file... ");
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
