#!/usr/bin/env bash
# Run Lab 01 Tasks 1-4 as Java MapReduce (local runner: mapreduce.framework.name=local in each Driver).
# Prerequisites: JDK 11+, Apache Hadoop 3.x on PATH (hadoop), Maven (mvn) to build the JAR.
# Outputs match the Python streaming script names under output/.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAVA_PROJ="$ROOT/hadoop/java/lab01-mapreduce"
JAR="$JAVA_PROJ/target/lab01-mapreduce-1.0.0.jar"
DATA="$ROOT/data"
OUT="$ROOT/output"
WORK="$OUT/.mr_work_java"

for cmd in hadoop mvn; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing '$cmd' on PATH. Install Hadoop 3.x and Maven, then retry." >&2
    exit 1
  fi
done

( cd "$JAVA_PROJ" && mvn -q package -DskipTests )

MERGED="$(mktemp)"
trap 'rm -f "$MERGED"' EXIT
{
  cat "$DATA/ratings_1.txt"
  echo
  cat "$DATA/ratings_2.txt"
} > "$MERGED"

mkdir -p "$OUT" "$WORK"

ABS_MERGED="$(cd "$(dirname "$MERGED")" && pwd)/$(basename "$MERGED")"
ABS_MOVIES="$DATA/movies.txt"
ABS_USERS="$DATA/users.txt"

echo "Task 1 (Java MapReduce, two stages)..."
hadoop jar "$JAR" ds200.lab01.task1.Task1Driver \
  "$ABS_MERGED" "$ABS_MOVIES" "$WORK" "$OUT/task1_movie_ratings.txt"

echo "Task 2..."
hadoop jar "$JAR" ds200.lab01.task2.Task2Driver \
  "$ABS_MERGED" "$ABS_MOVIES" "$WORK" "$OUT/task2_genre_ratings.txt"

echo "Task 3..."
hadoop jar "$JAR" ds200.lab01.task3.Task3Driver \
  "$ABS_MERGED" "$ABS_USERS" "$ABS_MOVIES" "$WORK" "$OUT/task3_gender_by_movie.txt"

echo "Task 4..."
hadoop jar "$JAR" ds200.lab01.task4.Task4Driver \
  "$ABS_MERGED" "$ABS_USERS" "$ABS_MOVIES" "$WORK" "$OUT/task4_age_groups_by_movie.txt"

# Same post-processing as scripts/run_hadoop_streaming_local.sh (sort full report lines by title).
sort -o "$OUT/task3_gender_by_movie.txt" "$OUT/task3_gender_by_movie.txt"
sort -o "$OUT/task4_age_groups_by_movie.txt" "$OUT/task4_age_groups_by_movie.txt"

echo "Done. Reports: $OUT/task1_movie_ratings.txt ... task4_age_groups_by_movie.txt"
