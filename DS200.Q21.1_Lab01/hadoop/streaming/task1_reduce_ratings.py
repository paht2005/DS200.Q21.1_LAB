#!/usr/bin/env python3
"""Task 1 - reduce: sum Rating and count per MovieID (shuffle key = MovieID)."""

from __future__ import annotations

import sys

# Hadoop groups consecutive lines with the same key (MovieID) after shuffle/sort.
current_mid: str | None = None
sum_r = 0.0
count = 0


def flush() -> None:
    global current_mid, sum_r, count
    if current_mid is not None:
        # Partial sums for one movie; stage-2 job joins titles and formats the report.
        print(f"{current_mid}\t{sum_r}\t{count}")
    current_mid = None
    sum_r = 0.0
    count = 0


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue
    mid, rating_s = parts
    try:
        r = float(rating_s)
    except ValueError:
        continue
    if current_mid is None:
        current_mid = mid
    elif mid != current_mid:
        flush()
        current_mid = mid
    sum_r += r
    count += 1

flush()
