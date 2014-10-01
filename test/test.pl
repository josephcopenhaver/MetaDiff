use strict;
use Cwd 'abs_path';
use File::Basename;
use File::Path qw(make_path remove_tree);
#use File::Spec;

my $testPath = dirname(abs_path($0));
my $rootPath = dirname($testPath);
my $tmpDir = "$testPath/tmp";

print "ROOTPATH=$rootPath\n";

my @cmds = (
	[
		1,
		sub
		{
			return (remove_tree($tmpDir) && make_path($tmpDir)) ? 1 : 0;
		}
	],
	[
		0,
		"perl",
		"$rootPath/metaDiff.pl",
		"snapshot",
		"$testPath/test1/a",
		"$tmpDir/a.db.sqlite"
	],
	[
		0,
		"perl",
		"$rootPath/metaDiff.pl",
		"snapshot",
		"$testPath/test1/b",
		"$tmpDir/b.db.sqlite"
	],
	[
		0,
		"perl",
		"$rootPath/metaDiff.pl",
		"diff",
		"$tmpDir/a.db.sqlite",
		"$tmpDir/b.db.sqlite"
	]
);

sub logCmd
{
	my $isNotFirst = 0;
	foreach (@_)
	{
		if ($isNotFirst)
		{
			print " ";
		}
		else
		{
			$isNotFirst = 1;
		}
		if (/["\s\r\n]/)
		{
			my $e = $_;
			$e =~ s/"/""/;
			print '"';
			print $e;
			print '"';
		}
		else
		{
			print $_;
		}
	}
	print "\n";
}

foreach (@cmds)
{
	if ($_->[0])
	{
		# perl command
		$_->[1]->() || die;
	}
	else
	{
		my @cmd = @$_[1..(scalar(@$_)-1)];
		logCmd(@cmd);
		(system(@cmd) >> 8) == 0 || die;
	}
}

__END__