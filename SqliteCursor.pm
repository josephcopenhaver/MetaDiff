package SqliteCursor;

use strict;
use CONST;


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
	my ($dbh, $statementsRef) = @{$_[0]};
	die unless defined($dbh);
	my $ref = $_[1];
	warn sprintf("E_PS: %s\n", sql_sprintf($ref->[CONST::IDX_CMD], quoteList($dbh, @_[2..$#_]))) if CONST::DEBUG;
	my $sth_ = $ref->[CONST::IDX_STH];
	my $sth;
	if (!defined($sth_))
	{
		$sth_ = {};
		$ref->[CONST::IDX_STH] = $sth_;
		$sth = undef;
	}
	else
	{
		$sth = $sth_->{$dbh};
	}
	if (!defined($sth))
	{
		my $_sth = undef;
		$sth = \$_sth;
		$sth_->{$dbh} = $sth;
		push(@$statementsRef, $sth_);
	}
	if (!defined($$sth))
	{
		$$sth = $dbh->prepare($ref->[CONST::IDX_CMD]) || die
	}
	my $_sth = $$sth;
	$_sth->execute(@_[2..$#_]) || die;
	return $_sth;
}

sub destroy
{
	my ($dbh, $statementsRef) = @{$_[0]};
	die unless defined($dbh);
	# clear out dbh
	$_[0]->[0] = undef;
	delete $activeInstances{$_[0]};
	while (my $sth_ref = pop(@$statementsRef))
	{
		my $sth = $sth_ref->{$dbh};
		delete $sth_ref->{$dbh};
		if (defined(my $_sth = $$sth))
		{
			$_sth->finish;
			$$sth = undef;
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
