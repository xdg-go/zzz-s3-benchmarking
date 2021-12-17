#!/usr/bin/env perl
use v5.28;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;
use feature 'signatures';
no warnings 'experimental::signatures';

use Path::Tiny;

my @sizes = qw(
  K001
  K004
  K016
  K064
  K256
  M001
  M004
  M016
  M032
  M064
  M128
  M256
);

# Assumes a 10GiB file of random data called 'bigrandom'

for my $s (@sizes) {
    my ( $unit, $digits ) = $s =~ /([KM])0*(\d+)/;

    my $filesize = $digits * 1024; # Digits are KiB
    $filesize *= 1024 if $unit eq "M";

    my $dataset_size = 1024**3;          # 1 GiB
    $dataset_size *= 10 if $unit eq "M"; # 10 GiB

    my $files_to_make = $dataset_size / $filesize;
    say "$s: Making $files_to_make files of $filesize bytes";

    for my $i ( 0 .. $files_to_make - 1 ) {
        my $base = sprintf( "%08x", $i );
        my $hash = substr( $base, -2 );
        my $path = "$s/$hash/$base";
        path($path)->touchpath;
        my $cmd = "dd bs=$filesize skip=$i count=1 if=bigrandom of=$path";
        say($cmd);
        system($cmd);
    }
}

