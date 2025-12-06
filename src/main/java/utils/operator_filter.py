#!/usr/bin/env python3
import sys
import json

if len(sys.argv) < 2:
    sys.stderr.write("Usage: operator_filter.py <grep_pattern>\n")
    sys.exit(1)

GREP_PATTERN = sys.argv[1]

def should_keep(line):
    return GREP_PATTERN in line # case sensitive substring match

for line in sys.stdin:
    try:
        if should_keep(line):
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
    except:
        continue