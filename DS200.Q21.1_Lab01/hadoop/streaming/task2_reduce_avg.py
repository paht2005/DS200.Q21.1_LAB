#!/usr/bin/env python3
"""Task 2 - reduce: average rating and count per Genre."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import fmt_rating

# Same grouping pattern as task1_reduce_ratings: one genre per key group.
current_genre: str | None = None
sum_r = 0.0
count = 0


def flush() -> None:
    global current_genre, sum_r, count
    if current_genre is not None and count > 0:
        avg = sum_r / count
        print(
            f"{current_genre}: {fmt_rating(avg)} (TotalRatings: {count})"
        )
    current_genre = None
    sum_r = 0.0
    count = 0


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue
    genre, rating_s = parts
    try:
        r = float(rating_s)
    except ValueError:
        continue
    if current_genre is None:
        current_genre = genre
    elif genre != current_genre:
        flush()
        current_genre = genre
    sum_r += r
    count += 1

flush()
