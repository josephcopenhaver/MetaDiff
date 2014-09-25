package SqliteCursor;

use strict;
use CONST;
#use Scalar::Util 'weaken';#Future use?


## HELPERS ##
sub quoteList
{
    my $dbh = $_[0];
    my @rval = ();
    my $e;
    foreach $e (@_[1..$#_])
    {
        push(@rval, $dbh->quote($e));
    }
    return @rval;
}

sub sql_sprintf
{
    my $rval = $_[0];
    my $c = () = $rval =~ /\?/g;
    
    $c == $#_ || die;
    return $rval if ($c == 0);
    
    $rval =~ s/%/%%/g;
    $rval =~ s/\?/%s/g;
    $c = sprintf($rval, @_[1..$#_]);
    return $c;
}


## CLASS STRUCT ##
my %activeInstances = ();

sub new
{
    my $class = $_[0];
    my $self = [$_[1], []];
    bless $self, $class;
    $activeInstances{$self} = $self;
    return $self;
}

sub execute
{
	my ($dbh, $arr_hash_sthPtr_by_dbh) = @{$_[0]};
	die unless defined($dbh);
	my $ref = $_[1];
	warn sprintf("E_PS: %s\n", sql_sprintf($ref->[CONST::IDX_CMD], quoteList($dbh, @_[2..$#_]))) if CONST::DEBUG;
	my $hash_sthPtr_by_dbh = $ref->[CONST::IDX_STH];
	my $sthPtr;
	if (!defined($hash_sthPtr_by_dbh))
	{
		$hash_sthPtr_by_dbh = {};
		$ref->[CONST::IDX_STH] = $hash_sthPtr_by_dbh;
		$sthPtr = undef;
	}
	else
	{
		$sthPtr = $hash_sthPtr_by_dbh->{$dbh};
	}
	if (!defined($sthPtr))
	{
		my $sth = undef;
		$sthPtr = \$sth;
		$hash_sthPtr_by_dbh->{$dbh} = $sthPtr;
		push(@$arr_hash_sthPtr_by_dbh, $hash_sthPtr_by_dbh);
	}
	if (!defined($$sthPtr))
	{
		$$sthPtr = $dbh->prepare($ref->[CONST::IDX_CMD]) || die
	}
	my $sth = $$sthPtr;
	$sth->execute(@_[2..$#_]) || die;
	return $sth;
}

sub destroy
{
	my ($dbh, $arr_hash_sthPtr_by_dbh) = @{$_[0]};
	die unless defined($dbh);
	# clear out dbh
	$_[0]->[0] = undef;
	delete $activeInstances{$_[0]};
	while (my $hash_sthPtr_by_dbh = pop(@$arr_hash_sthPtr_by_dbh))
	{
		my $sthPtr = $hash_sthPtr_by_dbh->{$dbh};
		delete $hash_sthPtr_by_dbh->{$dbh};
		if (defined(my $sth = $$sthPtr))
		{
			$sth->finish;
			$$sthPtr = undef;
		}
	}
}

sub lastrowid
{
	return $_[0]->[0]->sqlite_last_insert_rowid();
}

sub destroyAll
{
    my $e;
    foreach $e (values %activeInstances)
    {
        $e->destroy();
    }
}



#EOF
1;
