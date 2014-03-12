#!/usr/bin/env perl

use strict;
use DateTime;

my %events = ();
while(<>){

	my ($timestamp, $matricola, $action, $called_station, $calling_station, $eap) = split (/;/,$_);
	if ( not exists $events{$matricola} ) {
		if ( $action eq "Start" ) {
			$events{$matricola}{1}{'Start'}=$timestamp;
		}
	} else {
		my @array_actions=sort(keys(%{ $events{$matricola} }));
		my $size=scalar keys(%{ $events{$matricola} }); 
		if ( $action eq "Start" ) {
			$size = $size + 1;
			$events{$matricola}{$size}{'Start'}=$timestamp;
		} elsif ($action eq "Stop")  {
			foreach (@array_actions){
				if (not exists $events{$matricola}{$_}{'Stop'}){
					$events{$matricola}{$_}{'Stop'}=$timestamp;
					print "$matricola;$events{$matricola}{$_}{'Start'};$events{$matricola}{$_}{'Stop'}\n";
					last;
				}
			}
		}
	}
}
close FILE;
