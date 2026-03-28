#!/usr/bin/env python3
"""Task 3 - reduce: Male / Female mean rating per MovieID; print with Title."""

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

# Per-MovieID group: accumulate sums for M and F separately.
current_mid: str | None = None
sum_m = sum_f = 0.0
cnt_m = cnt_f = 0


def flush() -> None:
    global current_mid, sum_m, sum_f, cnt_m, cnt_f
    if current_mid is None:
        return
    title = movies.get(current_mid, current_mid)
    male_s = "N/A" if cnt_m == 0 else fmt_rating(sum_m / cnt_m)
    female_s = "N/A" if cnt_f == 0 else fmt_rating(sum_f / cnt_f)
    print(f"{title}: {male_s}, {female_s}")
    current_mid = None
    sum_m = sum_f = 0.0
    cnt_m = cnt_f = 0


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    mid, gender, rating_s = parts[0], parts[1], parts[2]
    try:
        r = float(rating_s)
    except ValueError:
        continue
    if current_mid is None:
        current_mid = mid
    elif mid != current_mid:
        flush()
        current_mid = mid
    if gender == "M":
        sum_m += r
        cnt_m += 1
    elif gender == "F":
        sum_f += r
        cnt_f += 1

flush()
