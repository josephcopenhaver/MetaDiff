use strict;
use Cwd 'abs_path';
use File::Basename;

my $testPath = dirname(abs_path($0));
my $rootPath = dirname($testPath);

print "ROOTPATH=$rootPath\n";

my @cmds = (
	[
		1,
		"mkdir",
		"tmp"
	],
	[
		0,
		"perl",
		"$rootPath/metaDiff.pl",
		"snapshot",
		"$testPath/test1/a",
		"$testPath/tmp/a.db.sqlite"
	],
	[
		0,
		"perl",
		"$rootPath/metaDiff.pl",
		"snapshot",
		"$testPath/test1/b",
		"$testPath/tmp/b.db.sqlite"
	],
	[
		0,
		"perl",
		"$rootPath/metaDiff.pl",
		"diff",
		"$testPath/tmp/a.db.sqlite",
		"$testPath/tmp/b.db.sqlite"
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
		if (/"/)
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
	my ($cmdStruct, $nonStrict) = ($_, $_->[0]);
	my @cmd = @$cmdStruct[1..(scalar(@$cmdStruct)-1)];
	logCmd(@cmd);
	(system(@cmd) >> 8) == 0 || $nonStrict || die;
}

__END__