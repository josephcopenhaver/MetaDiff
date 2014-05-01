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
my $jitCreated = 0;

sub new
{
    my $class = $_[0];
    my $dbh = $_[1];
    my @statements;
    my $self = {
        execute => sub
        {
            my $ref = $_[1];
            warn sprintf("E_PS: %s\n", sql_sprintf($ref->{'cmd'}, quoteList($dbh, @_[2..$#_]))) if CONST::DEBUG;
            my $sth = $ref->{'sth'};
            if (!defined($sth))
            {
                my $_sth = undef;
                $sth = \$_sth;
                $ref->{'sth'} = $sth;
                push(@statements, $ref);
            }
            if (!defined($$sth))
            {
                $$sth = $dbh->prepare($ref->{'cmd'}) || die
            }
            my $_sth = $$sth;
            $_sth->execute(@_[2..$#_]) || die;
            return $_sth;
        },
        destroy => sub
        {
			my $self = $_[0];
            die unless defined($dbh);
			my $refList;
            $dbh = undef;
            delete $activeInstances{$_[0]};
            while (my $sth_ref = pop(@statements))
            {
                my $sth = $sth_ref->{'sth'};
                delete $sth_ref->{'sth'};
                if (defined(my $_sth = $$sth))
                {
                    $_sth->finish;
                    $$sth = undef;
                }
            }
			$refList = $self->{"ref_list"};
			if (defined($refList))
			{
				delete $self->{"ref_list"};
				my $e;
				foreach $e (@$refList)
				{
					delete $e->{$self};
				}
			}
        },
        lastrowid => sub
        {
            return $dbh->sqlite_last_insert_rowid();
        }
    };
    if (!$jitCreated)
    {
        $jitCreated = 1;
        no strict 'refs';
        foreach my $k (keys %$self)
        {
            my $absRefName = sprintf("%s::%s", $class, $k);
            if (ref($self->{$k}) eq 'CODE')
            {
                *{$absRefName} = sub { return $_[0]->{$k}->(@_); };
            }
            else
            {
                *{$absRefName} = sub { return $_[0]->{$k}; };
            }
        }
        use strict 'refs';
    }
    bless $self, $class;
    $activeInstances{$self} = $self;
    return $self;
}

sub addHashRef
{
	my $self = $_[0];
	my $refList = $self->{"ref_list"};
	if (!defined($refList))
	{
		$refList = [];
		$self->{"ref_list"} = $refList;
	}
	push(@$refList, $_[1]);
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
