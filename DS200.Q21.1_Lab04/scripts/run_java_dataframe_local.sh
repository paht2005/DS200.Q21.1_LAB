#!/usr/bin/env bash
# Run DS200 Lab04 tasks 1-5 + 6,7,10 with Java Spark DataFrame in local mode.
# Prerequisites: Java 11+, Maven, and Spark (spark-submit on PATH).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_PROJ="$ROOT/spark/java/lab04-dataframe"
JAR="$SPARK_PROJ/target/lab04-dataframe-1.0.0.jar"
DATA="$ROOT/data"
OUT="$ROOT/output"

for cmd in spark-submit mvn; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing '$cmd' on PATH. Install Apache Spark and Maven, then retry." >&2
    exit 1
  fi
done

mkdir -p "$OUT"

echo "Building JAR..."
(
  cd "$SPARK_PROJ"
  mvn -q package -DskipTests
)
echo "Build complete: $JAR"
echo ""

echo "Task 1 - Load CSV files with inferSchema..."
spark-submit --master local[*] --class ds200.lab04.task1.Task1App "$JAR" \
  "$DATA" "$OUT/task1_load_datasets.txt"

echo "Task 2 - Total orders, customers, and sellers..."
spark-submit --master local[*] --class ds200.lab04.task2.Task2App "$JAR" \
  "$DATA" "$OUT/task2_overall_stats.txt"

echo "Task 3 - Orders by country (descending)..."
spark-submit --master local[*] --class ds200.lab04.task3.Task3App "$JAR" \
  "$DATA" "$OUT/task3_orders_by_country.txt"

echo "Task 4 - Orders by year/month..."
spark-submit --master local[*] --class ds200.lab04.task4.Task4App "$JAR" \
  "$DATA" "$OUT/task4_orders_by_year_month.txt"

echo "Task 5 - Review score statistics..."
spark-submit --master local[*] --class ds200.lab04.task5.Task5App "$JAR" \
  "$DATA" "$OUT/task5_review_stats.txt"

echo "Task 6 - Revenue 2024 by product category..."
spark-submit --master local[*] --class ds200.lab04.task6.Task6App "$JAR" \
  "$DATA" "$OUT/task6_revenue_2024_by_category.txt"

echo "Task 7 - Top-selling products + avg review score..."
spark-submit --master local[*] --class ds200.lab04.task7.Task7App "$JAR" \
  "$DATA" "$OUT/task7_top_products.txt"

echo "Task 10 - Seller ranking by revenue..."
spark-submit --master local[*] --class ds200.lab04.task10.Task10App "$JAR" \
  "$DATA" "$OUT/task10_seller_ranking.txt"

echo ""
echo "All tasks complete. Reports written to: $OUT"
