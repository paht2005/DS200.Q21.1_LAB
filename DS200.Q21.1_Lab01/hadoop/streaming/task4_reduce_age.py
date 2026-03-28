#!/usr/bin/env python3
"""Task 4 - reduce: mean rating per age bucket per MovieID; print with Title."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import fmt_rating, load_movies_from_path

MOVIES_PATH = os.environ.get("LAB01_MOVIES_FILE", "data/movies.txt")
if len(sys.argv) > 1:
    MOVIES_PATH = sys.argv[1]

movies = load_movies_from_path(MOVIES_PATH)

BUCKET_ORDER = ["0-18", "18-35", "35-50", "50+"]

current_mid: str | None = None
# Per-bucket sum and count while reducing one MovieID group.
sums: dict[str, float] = {b: 0.0 for b in BUCKET_ORDER}
counts: dict[str, int] = {b: 0 for b in BUCKET_ORDER}


def flush() -> None:
    global current_mid, sums, counts
    if current_mid is None:
        return
    title = movies.get(current_mid, current_mid)
    parts: list[str] = []
    for b in BUCKET_ORDER:
        if counts[b] == 0:
            parts.append(f"{b}: N/A")
        else:
            parts.append(f"{b}: {fmt_rating(sums[b] / counts[b])}")
    print(f"{title}: [{', '.join(parts)}]")
    current_mid = None
    sums = {b: 0.0 for b in BUCKET_ORDER}
    counts = {b: 0 for b in BUCKET_ORDER}


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    mid, bucket, rating_s = parts[0], parts[1], parts[2]
    if bucket not in sums:
        continue
    try:
        r = float(rating_s)
    except ValueError:
        continue
    if current_mid is None:
        current_mid = mid
    elif mid != current_mid:
        flush()
        current_mid = mid
    sums[bucket] += r
    counts[bucket] += 1

flush()
