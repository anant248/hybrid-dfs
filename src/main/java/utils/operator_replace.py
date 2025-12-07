#!/usr/bin/env python3
import sys
import json
import re

# Expect: operator_replace.py "<oldword newword>" OR "<oldword,newword>" etc.
if len(sys.argv) != 2:
    sys.stderr.write("Usage: operator_replace.py \"oldword newword\"\n")
    sys.exit(1)

raw = sys.argv[1].strip()

# split on space, comma, colon, semicolon, or multiple spaces
parts = re.split(r"[ ,:;]+", raw)

if len(parts) != 2:
    sys.stderr.write(f"Error: expected 2 words, got {len(parts)} from argument: {raw}\n")
    sys.exit(1)

old_word, new_word = parts[0], parts[1]

# case-insensitive regex
pattern = re.compile(re.escape(old_word), re.IGNORECASE)

for line in sys.stdin:
    try:
        tup = json.loads(line.strip())
        text = tup.get("line", "")

        replaced = pattern.sub(new_word, text)

        out = dict(tup)
        out["line"] = replaced

        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()

    except Exception:
        continue