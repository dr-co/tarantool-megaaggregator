use utf8;
use strict;
use warnings;

package DR::Tnt::MegaAgg::Test;
use base qw(Exporter);
our @EXPORT = qw(tnt tnti);
use feature 'state';
use DR::Tnt::Test;
use DR::Tnt;
use File::Spec::Functions 'catfile';
use File::Basename 'dirname';

sub tnti() {
    state $tnti;
    return $tnti if $tnti;

    my $dir = dirname dirname dirname dirname __FILE__;
    my $lua = catfile $dir, 'lua/agg.lua';

    $tnti = start_tarantool
                -lua => $lua;


    die $tnti->log unless $tnti->is_started;

    $tnti;
}

sub tnt() {
    state $tnt;
    
    return $tnt if $tnt;

    $tnt = tarantool
                driver      => 'coro',
                host        =>  '127.0.0.1',
                user        => 'test',
                password    => 'test',
                port        => tnti->port;

    $tnt;

}

1;
