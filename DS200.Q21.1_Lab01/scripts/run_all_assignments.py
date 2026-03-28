#!/usr/bin/env python3
"""Run tasks 1–4 and write reports under ``output/``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow `from lab01...` when the script is run from the repo root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from lab01.analytics import (
    task1_average_ratings_per_movie,
    task2_average_ratings_per_genre,
    task3_ratings_by_gender_per_movie,
    task4_ratings_by_age_group_per_movie,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DS200 Lab 01 — export task outputs as .txt files",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Folder with movies.txt and ratings_*.txt (default: <repo>/data or LAB01_DATA_DIR)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=PROJECT_ROOT / "output",
        help="Output directory for .txt reports",
    )
    args = parser.parse_args()
    data_dir = args.data_dir.resolve() if args.data_dir else None
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)

    # Each callable returns (report_text, dataframe); we only persist the text
    tasks = [
        ("task1_movie_ratings.txt", task1_average_ratings_per_movie),
        ("task2_genre_ratings.txt", task2_average_ratings_per_genre),
        ("task3_gender_by_movie.txt", task3_ratings_by_gender_per_movie),
        ("task4_age_groups_by_movie.txt", task4_ratings_by_age_group_per_movie),
    ]
    for name, fn in tasks:
        text, _df = fn(data_dir)
        path = out / name
        path.write_text(text + "\n", encoding="utf-8")
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
