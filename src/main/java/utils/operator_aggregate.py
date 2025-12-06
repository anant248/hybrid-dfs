#!/usr/bin/env python3
import sys
import json
import csv
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
        # use CSV reader to correctly handle commas inside quotes
        parts = next(csv.reader([line]))

        if COL_IDX < len(parts):
            key = parts[COL_IDX]
        else:
            key = "" # handle missing column
            
        key = key if key != "" else ""
        agg_val = update(key, 1)
        
        # Output as CSV: key,count
        out = f"{key},{agg_val}"
        
        sys.stdout.write(out + "\n")
        sys.stdout.flush()

    except:
        continue