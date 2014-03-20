package CSV_IO;

use strict;
use FileHandle;
use CONST;
use feature "state";


## HELPERS ##
sub unescape
{
    my $e = $_[0];
    die if (length($e) < 2);
    substr($e, 0, 1) eq '"' || die;
    substr($e, -1, 1) eq '"' || die;
    $e = substr($e, 1, -1);
    die if ($e =~ /[^"]"(?:"")*[^"]/m
        || $e =~ /^"(?:"")*[^"]/m
        || $e =~ /[^"]"(?:"")*$/m
        || $e =~ /^"(?:"")*$/m);
    $e =~ s/""/"/g;
    return $e;
}


## CLASS STRUCT ##
sub new
{
    state $anonymousNane = "<anonymous>";
    warn "Creating CSV IO...\n" if CONST::DEBUG;
    my ($class, $mode, $filename) = @_;
    my %options = (@_[3..$#_]);
    my $fh;
    if (defined($fh = $options{'fh'}))
    {
        warn "Using pre-opened fh\n" if CONST::DEBUG;
        $fh || die;
        if (!defined($filename))
        {
            warn "Using anonymous filename\n" if CONST::DEBUG;
            $filename = $anonymousNane;
        }
    }
    else
    {
        $fh = undef;
    }
    
    if (!defined($filename) && length($mode) > 1)
    {
        $filename = substr($mode, 1);
        $mode = substr($mode, 0, 1);
    }
    $mode eq '<' || $mode eq '>' || die;
    if (!defined($fh))
    {
        $fh = FileHandle->new($filename, ($mode eq '<') ? 'r' : 'w') || die;
        binmode($fh) || die;
    }
    my $self = {
        'fh' => $fh,
        'fm' => $mode,
        'fn' => $filename,
        'nc' => undef
    };
    if ($mode eq '<')
    {
        $self->{'eof'} = 0;
    }
    bless $self, $class;
    return $self;
}

sub close
{
    my $self = $_[0];
    my $fh = $self->{'fh'};
    return if !defined($fh);
    delete $self->{'fh'};
    close($fh) || die;
}

sub readrow
{
    my $self = $_[0];
    if ($self->{'eof'})
    {
        return undef;
    }
    $self->{'fm'} eq '<' || die;
    my $fh = $self->{'fh'} || die;
    my $line = <$fh>;
    if (!defined($line))
    {
        $self->{'eof'} = 1;
        return undef;
    }
    my $nc = $self->{'nc'};
    my $modDQ = 0;
    my @lines = ();
    my $column;
    my @columns = ();
    while (defined($line) && ($modDQ = (($modDQ + (() = $line =~ /"/g)) % 2)) != 0)
    {
        push(@lines, $line);
        $line = <$fh>;
    }
    defined($line) || die;
    $line = join('', @lines, $line);
    while ($line ne '')
    {
        if ($line =~ /^"/)
        {
            # looking for close of quoted section
            if ($line =~ /^((?:"")+)(?:,|\r\n|\r|\n)/m)
            {
                $column = $1;
                $line = $';
                $column = unescape($column);
            }
            elsif ($line =~ /^(.*?[^"]"(?:"")*)(?:,|\r\n|\r|\n)/m)
            {
                $column = $1;
                $line = $';
                $column = unescape($column);
            }
            else
            {
                die;
            }
        }
        else
        {
            # looking for close of non-quoted section
            if ($line =~ /(?:,|\r\n|\r|\n)/m)
            {
                $column = $`;
                $line = $';
            }
            else
            {
                die;
            }
        }
        push(@columns, $column);
    }
    
    my $_nc = scalar(@columns);
    
    if (defined($nc))
    {
        $nc == $_nc || die;
    }
    else
    {
        $_nc || die;
        $self->{'nc'} = $_nc;
    }
    
    if ($_nc <= 0)
    {
        $self->{'eof'} = 1;
        return undef;
    }
    
    return @columns;
}

sub writerow
{
    state $undefReplacement = '';
    warn "Writing row...\n" if CONST::DEBUG;
    my $self = $_[0];
    $self->{'fm'} eq '>' || die;
    my $fh = $self->{'fh'} || die;
    my $nc = $self->{'nc'};
    if (defined($nc))
    {
        $nc == $#_ || die;
    }
    else
    {
        $self->{'nc'} = $nc;
    }
    my @row = @_[1..$#_];
    my $firstPrint = 1;
    my $escape;
    my $wrap;
    if ($#row == 0 && (!defined(($escape = $row[0])) || $escape eq ''))
    {
        $fh->print("\"\"\n") || die;
        warn "ROW: \"\"\n:EOR\n" if CONST::DEBUG;
        return;
    }
    print STDERR 'ROW: ' if CONST::DEBUG;
    foreach (@row)
    {
        if (!defined($_))
        {
            $_ = $undefReplacement;
        }
        $escape = (/"/);
        $wrap = ($escape || /[,\r\n]/);
        if ($wrap)
        {
            if ($escape)
            {
                $_ =~ s/"/""/g;
            }
            $_ = sprintf('"%s"', $_);
        }
        if ($firstPrint)
        {
            $firstPrint = 0;
        }
        else
        {
            $fh->print(',') || die;
            print STDERR ', ' if CONST::DEBUG;
        }
        $fh->print($_) || die;
        print STDERR $_ if CONST::DEBUG;
    }
    $firstPrint && die;
    $fh->print("\n") || die;
    warn "\n:EOR\n" if CONST::DEBUG;
}



#EOF
1;