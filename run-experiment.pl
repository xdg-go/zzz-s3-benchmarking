#!/usr/bin/env perl
use v5.28;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;
use feature 'signatures';
no warnings 'experimental::signatures';

my $datapoints = 20;
my $instance   = "c5n.2xlarge";

my %goroutines_per_set = (
    K001 => [ 8, 16, 32, 64, 128, 256, 512 ], # OOMs at 1024+
    K004 => [ 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 ],
    K016 => [ 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 ],
    K064 => [ 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 ],
    K256 => [ 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 ],
    M001 => [ 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 ],
    M004 => [ 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 2560 ],
    M016 => [ 8, 16, 32, 64, 128, 256, 512, 640 ],
    M032 => [ 8, 16, 32, 64, 128, 256, 320 ],
    M064 => [ 8, 16, 32, 64, 128, 160 ],
    M128 => [ 8, 16, 32, 64, 80 ],
    M256 => [ 8, 16, 32, 40 ],
);

for my $s ( reverse sort keys %goroutines_per_set ) {
    my ( $unit, $digits ) = $s =~ /([KM])0*(\d+)/;

    my $dataset_size = 1024;             # 1000 MB
    $dataset_size *= 10 if $unit eq "M"; # 10 GiB

    for my $g ( $goroutines_per_set{$s}->@* ) {
        my $cmd =
          "./main --instance=$instance --download=$dataset_size --set=$s --goroutines=$g --count=$datapoints | tee -a $s.out";
        say($cmd);
        system($cmd) == 0 or exit 1;
    }
}
