#!/usr/bin/env python3
"""Task 1 - reduce: format lines + highest-rated movie with at least 5 ratings (cleanup-style)."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import fmt_rating

REPORT_KEY = "TASK1_REPORT"
MIN_RATINGS = 5

# Collect all (title, avg, count) in one reducer pass, then sort and pick global max (>= MIN_RATINGS).
rows: list[tuple[str, float, int]] = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, _, rest = line.partition("\t")
    if key != REPORT_KEY:
        continue
    bits = rest.split("\t")
    if len(bits) < 3:
        continue
    title, avg_s, cnt_s = bits[0], bits[1], bits[2]
    try:
        avg = float(avg_s)
        cnt = int(cnt_s)
    except ValueError:
        continue
    rows.append((title, avg, cnt))

rows.sort(key=lambda t: t[0])
for title, avg, cnt in rows:
    print(f"{title} AverageRating: {fmt_rating(avg)} (TotalRatings: {cnt})")

print()
eligible = [t for t in rows if t[2] >= MIN_RATINGS]
if not eligible:
    print("No movie has at least 5 ratings.")
else:
    best_title, best_avg, _best_cnt = max(eligible, key=lambda t: t[1])
    print(
        f"{best_title} is the highest rated movie with an average rating of "
        f"{fmt_rating(best_avg)} among movies with at least 5 ratings."
    )
