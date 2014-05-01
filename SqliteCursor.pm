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
			die unless defined($dbh);
			my $ref = $_[1];
            warn sprintf("E_PS: %s\n", sql_sprintf($ref->{'cmd'}, quoteList($dbh, @_[2..$#_]))) if CONST::DEBUG;
            my $sth_ = $ref->{'sth'};
			my $sth;
			if (!defined($sth_))
			{
				$sth_ = {};
				$ref->{'sth'} = $sth_;
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
                push(@statements, $sth_);
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
			die unless defined($dbh);
			my $dbh_old = $dbh;
            # clear out dbh
			$dbh = undef;
            delete $activeInstances{$_[0]};
            while (my $sth_ref = pop(@statements))
            {
                my $sth = $sth_ref->{$dbh_old};
                delete $sth_ref->{$dbh_old};
                if (defined(my $_sth = $$sth))
                {
                    $_sth->finish;
                    $$sth = undef;
                }
            }
        },
        lastrowid => sub
        {
			die unless defined($dbh);
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
