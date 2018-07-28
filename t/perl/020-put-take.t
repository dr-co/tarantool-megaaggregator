#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib t/perl/lib);

use Test::More tests    => 12;
use Encode qw(decode encode);


BEGIN {
    use_ok 'DR::Tnt::MegaAgg::Test';
    use_ok 'DR::Tnt::MegaAgg';
    use_ok 'Coro';
    use_ok 'Coro::AnyEvent';

}

my $agg = new DR::Tnt::MegaAgg tnt => tnt;
ok $agg => 'instance created';

is $agg->push('tube', 'test', persistent => 0), 'test', 'push';
is_deeply
    $agg->push('tube', { привет => 'медвед' }, persistent => 1),
    { привет => 'медвед' },
    'push hash';

is_deeply
    $agg->push('tube', [ привет => 'медвед' ], persistent => 0),
    [ привет => 'медвед' ],
    'push array';

is_deeply $agg->take('tube', 500, 10, timeout => .1 ), [], 'take timeout';

Coro::AnyEvent::sleep .5;
is_deeply $agg->take('tube', 500, .1),
    [
        'test',
        { 'привет' => 'медвед' },
        [ 'привет' => 'медвед' ]
    ], 'take after timeout';


is_deeply tnt->select('MegaAggMemOnly', 'id', []), [], 'space is empty';
is_deeply tnt->select('MegaAgg', 'id', []), [], 'space is empty';
