#!/usr/bin/env python3
"""Task 3 - map: join ratings with users; emit MovieID, Gender, Rating."""

from __future__ import annotations

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from lab01_parse import load_users_from_path, parse_rating_line

USERS_PATH = os.environ.get("LAB01_USERS_FILE", "data/users.txt")
if len(sys.argv) > 1:
    USERS_PATH = sys.argv[1]

users = load_users_from_path(USERS_PATH)

# Key = MovieID avoids tab characters inside movie titles during shuffle.
for raw in sys.stdin:
    parsed = parse_rating_line(raw)
    if not parsed:
        continue
    uid, mid, rating = parsed
    info = users.get(uid)
    if not info:
        continue
    gender, _age = info
    if gender not in ("M", "F"):
        continue
    print(f"{mid}\t{gender}\t{rating}")
