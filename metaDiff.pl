use strict;
use warnings;
use DBI;
use File::Spec;
use feature "state";
use FileHandle;
use File::Basename;
use Fcntl qw(:seek);
use File::Temp qw/ tempfile /;#tempdir

use constant
{
	DEBUG => 1,
	DEBUG2 => 1, # may introduce side effects, but generally provides more verbose debug info
	TYPE_DIR => (1 << 0),
	TYPE_FILE => (1 << 1),
	TYPE_SYMLINK => (1 << 2)
};

use constant
{
	SCRIPT_CREATE_PATH_NAMES_TABLE => 'CREATE TABLE path_names(name_id integer PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE);',
	SCRIPT_CREATE_SNAPSHOT_DB_BASIS => 'CREATE TABLE path_elements(element_id integer PRIMARY KEY NOT NULL, type_id integer NOT NULL REFERENCES path_types(type_id) ON DELETE CASCADE, name_id integer NOT NULL REFERENCES path_names(name_id) ON DELETE CASCADE);
CREATE TABLE element_parents(element_id integer PRIMARY KEY NOT NULL, parent_id integer REFERENCES path_elements(element_id) ON DELETE CASCADE, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE); --NULL parent indicates root level
CREATE TABLE element_hash(element_id integer PRIMARY KEY NOT NULL, hash TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);
CREATE TABLE element_lastmodified(element_id integer PRIMARY KEY NOT NULL, last_modified TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);
CREATE TABLE element_size(element_id integer PRIMARY KEY NOT NULL, size integer CHECK(size>=0), FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);
CREATE TABLE element_link(element_id integer PRIMARY KEY NOT NULL, target TEXT NOT NULL, FOREIGN KEY(element_id) REFERENCES path_elements(element_id) ON DELETE CASCADE);',
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
	SCRIPT_CREATE_META_INFO_TABLE => sprintf("%s\n%s", 'CREATE TABLE path_types(type_id integer PRIMARY KEY NOT NULL, type_description TEXT UNIQUE);', SCRIPT_CREATE_PATH_NAMES_TABLE)
};

use constant
{
	SCRIPT_CREATE_SNAPSHOT_DB => sprintf("%s%s", SCRIPT_CREATE_META_INFO_TABLE, SCRIPT_CREATE_SNAPSHOT_DB_BASIS),
	SCRIPT_INIT_SNAPSHOT_DB => sprintf('INSERT INTO path_types SELECT %d AS type_id, \'directory\' AS type_description UNION SELECT %d,\'file\' UNION SELECT %d,\'symlink\';', TYPE_DIR, TYPE_FILE, TYPE_SYMLINK)
};

my $createSnap2Tables = [];
my $renameSnap1Tables = [];

foreach my $line (split(/\r?\n/, SCRIPT_CREATE_SNAPSHOT_DB_BASIS))
{
	foreach (@{+COMPARABLE_TABLES})
	{
		$line =~ s/(\Q$_\E)/$1.'2'/e;
	}
	push(@$createSnap2Tables, $line);
}
$createSnap2Tables = join("\n", @$createSnap2Tables);
foreach (@{+COMPARABLE_TABLES})
{
	push(@$renameSnap1Tables, sprintf("ALTER TABLE %s RENAME TO %s2;", $_, $_));
}
$renameSnap1Tables = join("\n", @$renameSnap1Tables);

use constant
{
	SCRIPT_CREATE_SNAPSHOT_2_TABLES => $createSnap2Tables,
	SCRIPT_RENAME_SNAPSHOT_1_TABLES => $renameSnap1Tables
};
$createSnap2Tables = undef;
$renameSnap1Tables = undef;


###


my $CSV_FD = undef;
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
		if (scalar(@args))
		{
			$doF->(@args);
		}
		else
		{
			undef(@args);
			$doF->();
		}
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
		my @files;
		my $d;
		my $s = scalar(@$refs);
		push(@$refs, undef);
		opendir($d, $_[0]) || die;
		$refs->[$s] = $d;
		@files = readdir $d;
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
	return @$r;
}

sub getPathType
{
	return TYPE_DIR; #TODO: complete
}

sub addElement
{
	#TODO: complete
}

sub recurseAddElement
{
	#TODO: complete
}

sub gatherElementInfo
{
	#TODO: complete
}

sub initDB
{
	my $dbh = $_[0];
	$dbh->do(SCRIPT_CREATE_SNAPSHOT_DB) || die;
	$dbh->do(SCRIPT_INIT_SNAPSHOT_DB) || die;
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

#TODO: Prepared statements:
# $sth = $dbh->prepare("") || die;

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
		warn("Cleaning up tmp csv file\n") if DEBUG;
		my $t = $csvInOut;
		warn("File: '$t'\n") if DEBUG2;
		$csvInOutFH->close();
		$csvInOutFH = undef;
		$csvInOut = undef;
		unlink($t) || die;
		warn("tmp csv file '$t' cleaned\n") if DEBUG;
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
		my $abs_src_path = File::Spec->rel2abs($rel_src_path);
		my $abs_src_root_path = $abs_src_path;
		($csvInOutFH, $csvInOut) = tempfile(SUFFIX => "_jcope_mdif.csv", UNLINK => 1);
		binmode($csvInOutFH) || die;
		if (getPathType($abs_src_path) != TYPE_DIR)
		{
			addElement($abs_src_path);
			$abs_src_root_path = dirname($abs_src_path);
		}
		else
		{
			foreach my $e (sort(getDirContents($abs_src_path)))
			{
				recurseAddElement(join_path($abs_src_path, $e));
			}
		}
		seek($csvInOutFH, SEEK_SET, 0) || die;
		$dbh = newRamDB();
		gatherElementInfo($abs_src_root_path);
		$cleanupCsv->();
		print("Saving data to file... ");
		$dbh->commit() || die;
		$dbh->sqlite_backup_to_file($out_file_path) || die;
		print("Done\n");
	};
	
	# not re-entrant
	$callDepth == 0 || die;
	
	return doF($doF, $doL, @_);
}


__END__
