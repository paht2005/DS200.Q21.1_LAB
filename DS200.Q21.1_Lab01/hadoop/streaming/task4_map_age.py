#!/usr/bin/env python3
"""Task 4 - map: join ratings with users; emit MovieID, AgeBucket, Rating."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import age_bucket, load_users_from_path, parse_rating_line

USERS_PATH = os.environ.get("LAB01_USERS_FILE", "data/users.txt")
if len(sys.argv) > 1:
    USERS_PATH = sys.argv[1]

users = load_users_from_path(USERS_PATH)

# Value layout: MovieID<TAB>bucket<TAB>rating (bucket labels match the assignment).
for raw in sys.stdin:
    parsed = parse_rating_line(raw)
    if not parsed:
        continue
    uid, mid, rating = parsed
    info = users.get(uid)
    if not info:
        continue
    _gender, age = info
    bucket = age_bucket(age)
    print(f"{mid}\t{bucket}\t{rating}")
