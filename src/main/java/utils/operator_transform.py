#!/usr/bin/env python3
import sys
import json
import csv

if len(sys.argv) < 1:
    sys.stderr.write("Usage: operator_transform.py\n")
    sys.exit(1)

def transform(t):
    line = t.get("line", "")

    # use CSV reader to correctly handle commas inside quotes
    parts = next(csv.reader([line]))

    trimmed = parts[:3]
    new_t = dict(t)
    new_t["line"] = ",".join(trimmed)
    return new_t

for line in sys.stdin:
    try:
        tup = json.loads(line.strip())
        out = transform(tup)
        if out is not None:
            sys.stdout.write(json.dumps(out) + "\n")
            sys.stdout.flush()
    except:
        continue