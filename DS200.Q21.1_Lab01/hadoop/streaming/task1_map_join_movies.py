#!/usr/bin/env python3
"""Task 1 - map (join): MovieID,sum,count -> single reduce key with title,avg,count payload."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import load_movies_from_path

# Same env var as shell scripts; Hadoop can set -cmdenv LAB01_MOVIES_FILE=...
MOVIES_PATH = os.environ.get("LAB01_MOVIES_FILE", "data/movies.txt")
if len(sys.argv) > 1:
    MOVIES_PATH = sys.argv[1]

movies = load_movies_from_path(MOVIES_PATH)
# Constant key so one reducer can see every movie line (like cleanup() after all keys).
REPORT_KEY = "TASK1_REPORT"

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) < 3:
        continue
    mid, sum_s, cnt_s = parts[0], parts[1], parts[2]
    try:
        s = float(sum_s)
        c = int(cnt_s)
    except ValueError:
        continue
    if c <= 0:
        continue
    title = movies.get(mid, mid)
    avg = s / c
    print(f"{REPORT_KEY}\t{title}\t{avg}\t{c}")
