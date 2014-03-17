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
	#TODO: complete
}

sub newRamDB
{
	my $dbh = DBI->connect(
		"dbi:SQLite:dbname=:memory:", # DSN
		"", # user
		"", # password
		{ RaiseError => 1 } # other
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
		$dbh->sqlite_backup_to_file($out_file_path) || die;
		print("Done\n");
	};
	
	# not re-entrant
	$callDepth == 0 || die;
	
	return doF($doF, $doL, @_);
}


__END__
