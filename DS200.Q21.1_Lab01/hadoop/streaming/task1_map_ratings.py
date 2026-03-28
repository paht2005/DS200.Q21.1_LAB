#!/usr/bin/env python3
"""Task 1 - map: emit MovieID and Rating from each ratings line (both rating files)."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import parse_rating_line

# Output: MovieID<TAB>Rating (one line per rating; same schema for ratings_1 and ratings_2).
for raw in sys.stdin:
    parsed = parse_rating_line(raw)
    if not parsed:
        continue
    _uid, movie_id, rating = parsed
    print(f"{movie_id}\t{rating}")
