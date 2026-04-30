#!/usr/bin/env perl
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
use strict;
use warnings;

# Normalize the top-level Kingpin "Commands:" section in generated help text.
#
# Usage:
#   command --help | scripts/normalize_help_commands.pl "help foo bar"
#
# Input:
#   - argv[0]: space-separated preferred top-level command order
#   - stdin:   full help text emitted by the CLI
#
# Output:
#   - same help text, but with command blocks under "Commands:" printed in
#     preferred order. Commands not listed in argv[0] are appended in their
#     original order so new commands remain visible in generated docs.

my @preferred_order = split / /, shift @ARGV;
my $help_text = join "", <STDIN>;

# Some subcommand help (e.g., `arma router --help`) has no "Commands:" section,
# only flags. Pass those through unchanged.
unless ($help_text =~ /^Commands:\n/m) {
    print $help_text;
    exit 0;
}

# Keep everything before "Commands:" unchanged. Only top-level command block
# order needs normalization.
my ($preamble, $commands_section) = split /^Commands:\n/m, $help_text, 2;
print $preamble, "Commands:\n";

my %command_blocks;
my @found_order;

# Kingpin renders each command block like:
#
#   router
#     run a router node
#
# Command blocks start with two spaces and continue until the next line that
# starts with two spaces followed by a non-space, or end of input. The /m flag
# makes ^ match line starts; /s lets . match newlines; /g scans all blocks.
while ($commands_section =~ /(^  \S.*?(?=^  \S|\z))/msg) {
    my $block = $1;
    my ($name) = $block =~ /^  (\S+)/;

    # Ensure exactly one blank line after each block so output remains stable.
    $block =~ s/\n+\z/\n\n/s;

    $command_blocks{$name} = $block;
    push @found_order, $name;
}

my %printed;

# Print commands in expected order first. Missing commands are ignored here so
# this script can be reused with binaries that may not expose every command.
for my $name (@preferred_order) {
    next unless exists $command_blocks{$name};

    print $command_blocks{$name};
    $printed{$name} = 1;
}

# Preserve any unexpected/new commands by appending them in original order.
for my $name (@found_order) {
    next if $printed{$name};

    print $command_blocks{$name};
}
