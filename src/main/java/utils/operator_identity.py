#!/usr/bin/env python3
import sys
import json

if len(sys.argv) < 1:
    sys.stderr.write("Usage: operator_identity.py\n")
    sys.exit(1)

IDENTITY = True

for line in sys.stdin:
    try:
        tup = json.loads(line.strip())
        if IDENTITY:
            sys.stdout.write(json.dumps(tup) + "\n")
            sys.stdout.flush()
    except:
        continue