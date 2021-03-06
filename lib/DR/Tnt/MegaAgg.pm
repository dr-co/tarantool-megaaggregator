=head1 NAME

DR::Tnt::MegaAgg - perl module for tarantool aggregator

=head1 SYNOPSIS

    use DR::Tnt;
    my $tnt = tarantool ...;
    
    use DR::Tnt::MegaAgg;
    my $agg = new DR::Tnt::MegaAgg tnt => $tnt;


    $agg->push(test => 123);
    $agg->push(test => 234, persistent => 0);
    $agg->pushlist(test => [ 1, 2, 3 ], persistent => 1);

    while() {
        my $list = $agg->take(test => 100, 5, timeout => .1);
        next unless @$list;
        process_list($list)
    }

=cut

use utf8;
use strict;
use warnings;

package DR::Tnt::MegaAgg;
use Mouse;
use Carp;

our $VERSION = '0.01';

has tnt =>
    is          => 'ro',
    isa         => 'Object',
    required    => 1;

sub push :method {
    my ($self, $tube, $data, %opts) = @_;
    
    my $o = {};
    for ($opts{ttl}) {
        next unless defined $_;
        unless (/^\d+(\.\d*)?$/) {
            croak "ttl must be a number > 0";
        }
        $o->{ttl} = 0 + $opts{ttl};
    }
    for ($opts{persistent}) {
        next unless defined $_;
        $o->{persistent} = $_ ? 1 : 0;
    }
    $self->tnt->call_lua('megaagg:push', $tube, $data, $o)->[0][-1];
}

sub push_list :method {
    my ($self, $tube, $list, %opts) = @_;
    croak 'list must be ARRAY' unless 'ARRAY' eq ref $list;
    return [] unless @$list;

    my $o = {};
    for ($opts{ttl}) {
        next unless defined $_;
        unless (/^\d+(\.\d*)?$/) {
            croak "ttl must be a number > 0";
        }
        $o->{ttl} = 0 + $opts{ttl};
    }
    for ($opts{persistent}) {
        next unless defined $_;
        $o->{persistent} = $_ ? 1 : 0;
    }
    
    [ map  { $_->[-1] }
        @{ $self->tnt->call_lua('megaagg:push_list', $tube, $list, $o) } ];
}

sub take {
    my ($self, $tube, $limit, $timeout, %opts) = @_;
    my $o = {};
    for ($opts{timeout}, $timeout) {
        next unless defined $_;
        unless (/^\d+(\.\d*)?$/) {
            croak "timeout must be a number > 0";
        }
    }
    for ($opts{limit}, $limit) {
        next unless defined $_;
        unless (/^\d+(\.\d*)?$/) {
            croak "timeout must be a number > 0";
        }
    }

    $o->{timeout} = $opts{timeout} if $opts{timeout};
    $o->{limit} = $opts{limit} if $opts{limit};
    [ map { $_->[-1] }
        @{ $self->tnt->call_lua('megaagg:take', $tube, $limit, $timeout, $o) } ]
}

__PACKAGE__->meta->make_immutable;

