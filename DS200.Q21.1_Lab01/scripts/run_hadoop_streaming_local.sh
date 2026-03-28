#!/usr/bin/env bash
# Local Hadoop Streaming-style pipeline: Unix sort stands in for the shuffle/sort phase.
# Produces the same four report files under output/ as the pandas reference script.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STREAM="$ROOT/hadoop/streaming"
DATA="$ROOT/data"
OUT="$ROOT/output"

export LAB01_MOVIES_FILE="$DATA/movies.txt"
export LAB01_USERS_FILE="$DATA/users.txt"

mkdir -p "$OUT"

combine_ratings() {
  # Ensure a newline between files (ratings_1 may lack a trailing newline; bare cat would glue lines).
  cat "$DATA/ratings_1.txt"
  echo
  cat "$DATA/ratings_2.txt"
}

echo "Task 1 (two MapReduce-style stages: aggregate ratings, then join + report)..."
combine_ratings \
  | python3 "$STREAM/task1_map_ratings.py" \
  | sort -t $'\t' -k1,1 \
  | python3 "$STREAM/task1_reduce_ratings.py" \
  | python3 "$STREAM/task1_map_join_movies.py" "$DATA/movies.txt" \
  | sort -t $'\t' -k1,1 \
  | python3 "$STREAM/task1_reduce_report.py" \
  > "$OUT/task1_movie_ratings.txt"

echo "Task 2 (genre explode + average)..."
combine_ratings \
  | python3 "$STREAM/task2_map_genre_rating.py" "$DATA/movies.txt" \
  | sort -t $'\t' -k1,1 \
  | python3 "$STREAM/task2_reduce_avg.py" \
  > "$OUT/task2_genre_ratings.txt"

echo "Task 3 (ratings join users; reducer joins titles)..."
combine_ratings \
  | python3 "$STREAM/task3_map_gender.py" "$DATA/users.txt" \
  | sort -t $'\t' -k1,1 \
  | python3 "$STREAM/task3_reduce_gender.py" "$DATA/movies.txt" \
  | sort \
  > "$OUT/task3_gender_by_movie.txt"

echo "Task 4 (age buckets + reducer joins titles)..."
combine_ratings \
  | python3 "$STREAM/task4_map_age.py" "$DATA/users.txt" \
  | sort -t $'\t' -k1,1 \
  | python3 "$STREAM/task4_reduce_age.py" "$DATA/movies.txt" \
  | sort \
  > "$OUT/task4_age_groups_by_movie.txt"

echo "Done. Reports: $OUT/task1_movie_ratings.txt ... task4_age_groups_by_movie.txt"
