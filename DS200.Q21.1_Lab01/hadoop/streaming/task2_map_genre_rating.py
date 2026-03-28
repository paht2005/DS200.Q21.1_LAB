#!/usr/bin/env python3
"""Task 2 - map: one rating line -> one intermediate row per genre (split Genres on |)."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import parse_movie_line, parse_rating_line

MOVIES_PATH = os.environ.get("LAB01_MOVIES_FILE", "data/movies.txt")
if len(sys.argv) > 1:
    MOVIES_PATH = sys.argv[1]

# Replicated join: small movies table in memory (also shipped with -files on a cluster).
# MovieID -> list of genre names
genres_by_mid: dict[str, list[str]] = {}
with open(MOVIES_PATH, encoding="utf-8", errors="replace") as f:
    for raw in f:
        parsed = parse_movie_line(raw)
        if not parsed:
            continue
        mid, _title, genres_s = parsed
        genres_by_mid[mid] = [g.strip() for g in genres_s.split("|") if g.strip()]

for raw in sys.stdin:
    parsed = parse_rating_line(raw)
    if not parsed:
        continue
    _uid, mid, rating = parsed
    for genre in genres_by_mid.get(mid, []):
        # Shuffle key = genre name; value = numeric rating for the reducer to average.
        print(f"{genre}\t{rating}")
