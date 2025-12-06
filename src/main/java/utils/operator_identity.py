#!/usr/bin/env python3
import sys
import json

if len(sys.argv) < 1:
    sys.stderr.write("Usage: operator_identity.py\n")
    sys.exit(1)

IDENTITY = True

for line in sys.stdin:
    try:
        if IDENTITY:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
    except:
        continue