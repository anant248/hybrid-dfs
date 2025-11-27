#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

if len(sys.argv) < 2:
    sys.stderr.write("Usage: operator_aggregate.py <column_index>\n")
    sys.exit(1)

COL_IDX = int(sys.argv[1])

state = defaultdict(int)

def update(key, value):
    """
    Example: sum aggregation
    """
    state[key] += value
    return state[key]

for line in sys.stdin:
    try:
        tup = json.loads(line.strip())

        line = tup.get("line", "")
        parts = line.split(",")
        if COL_IDX < len(parts):
            key = parts[COL_IDX]
        else:
            key = ""
        key = key if key != "" else ""
        agg_val = update(key, 1)
        out = {"key": key, "count": agg_val}
        
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()

    except:
        continue