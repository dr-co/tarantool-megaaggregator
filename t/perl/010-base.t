#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib t/perl/lib);

use Test::More tests    => 4;
use Encode qw(decode encode);


BEGIN {
    use_ok 'DR::Tnt::MegaAgg::Test';

}

ok tnt, 'tnt';
ok tnt->ping, 'ping';
like tnti->log, qr{MegaAgg started}, 'MegaAgg started';
