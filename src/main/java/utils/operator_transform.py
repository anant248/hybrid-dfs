#!/usr/bin/env python3
import sys
import json

if len(sys.argv) < 1:
    sys.stderr.write("Usage: operator_transform.py\n")
    sys.exit(1)

def transform(t):
    line = t.get("line", "")
    parts = line.split(",")
    trimmed = parts[:3]
    t["line"] = ",".join(trimmed)
    return t

for line in sys.stdin:
    try:
        tup = json.loads(line.strip())
        out = transform(tup)
        if out is not None:
            sys.stdout.write(json.dumps(out) + "\n")
            sys.stdout.flush()
    except:
        continue