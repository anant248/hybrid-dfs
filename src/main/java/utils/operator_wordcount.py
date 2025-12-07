#!/usr/bin/env python3
import sys
import json
import re
from collections import defaultdict

state = defaultdict(int)

for line in sys.stdin:
    try:
        tup = json.loads(line.strip())
        text = tup.get("line", "")

        # tokenize: split on non-letters, lowercase everything
        words = re.split(r"[^A-Za-z]+", text.lower())

        for w in words:
            if not w:
                continue
            state[w] += 1

            out = dict(tup)
            out["word"] = w
            out["count"] = state[w]

            sys.stdout.write(json.dumps(out) + "\n")
            sys.stdout.flush()

    except Exception:
        continue