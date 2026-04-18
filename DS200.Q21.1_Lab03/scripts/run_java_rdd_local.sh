#!/usr/bin/env bash
# Run DS200 Lab03 tasks 1-6 with Java Spark RDD in local mode.
# Prerequisites: Java 11+, Maven, and Spark (spark-submit on PATH).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_PROJ="$ROOT/spark/java/lab03-rdd"
JAR="$SPARK_PROJ/target/lab03-rdd-1.0.0.jar"
DATA="$ROOT/data"
OUT="$ROOT/output"

for cmd in spark-submit mvn; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing '$cmd' on PATH. Install Apache Spark and Maven, then retry." >&2
    exit 1
  fi
done

mkdir -p "$OUT"

(
  cd "$SPARK_PROJ"
  mvn -q package -DskipTests
)

echo "Task 1 - movie average and rating count..."
spark-submit --master local[*] --class ds200.lab03.task1.Task1App "$JAR" \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task1_movie_ratings.txt" "5"

echo "Task 2 - average rating by genre..."
spark-submit --master local[*] --class ds200.lab03.task2.Task2App "$JAR" \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task2_genre_ratings.txt"

echo "Task 3 - average rating by gender per movie..."
spark-submit --master local[*] --class ds200.lab03.task3.Task3App "$JAR" \
  "$DATA/movies.txt" "$DATA/users.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task3_gender_by_movie.txt"

echo "Task 4 - average rating by age group per movie..."
spark-submit --master local[*] --class ds200.lab03.task4.Task4App "$JAR" \
  "$DATA/movies.txt" "$DATA/users.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task4_age_groups_by_movie.txt"

echo "Task 5 - average rating by occupation..."
spark-submit --master local[*] --class ds200.lab03.task5.Task5App "$JAR" \
  "$DATA/users.txt" "$DATA/occupation.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task5_occupation_ratings.txt"

echo "Task 6 - average rating by year..."
spark-submit --master local[*] --class ds200.lab03.task6.Task6App "$JAR" \
  "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task6_yearly_ratings.txt"

echo "Done. Reports written to: $OUT"
