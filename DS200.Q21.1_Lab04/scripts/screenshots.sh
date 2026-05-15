#!/usr/bin/env bash
# DS200 Lab 04 - Screenshot helper
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
STEP="${1:-all}"

header() {
  echo ""
  echo "================================================================"
  echo "  DS200 Lab 04 - Screenshot $1: $2"
  echo "  Student: Phat Cong Nguyen - 23521143    user: $(whoami)"
  echo "================================================================"
  echo ""
}

pause() {
  if [[ "$STEP" == "all" ]]; then
    echo ">>> Take a screenshot now, then press Enter..."
    read -r < /dev/tty
  fi
}

if [[ "$STEP" == "all" || "$STEP" == "1" ]]; then
  header 1 "Identity and Environment"
  echo "whoami: $(whoami)"
  echo "Student: Phat Cong Nguyen - 23521143"
  echo "Date: $(date)"
  java -version 2>&1 || echo "(java not found)"
  mvn -version 2>&1 | head -1 || echo "(mvn not found)"
  spark-submit --version 2>&1 | head -3 || echo "(spark-submit not found)"
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "2" ]]; then
  header 2 "Task 1 - Load CSV files with inferSchema"
  cat output/task1_load_datasets.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "3" ]]; then
  header 3 "Task 2 - Total orders, customers, sellers"
  cat output/task2_overall_stats.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "4" ]]; then
  header 4 "Task 3 - Orders by country (descending)"
  cat output/task3_orders_by_country.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "5" ]]; then
  header 5 "Task 4 - Orders by year and month"
  cat output/task4_orders_by_year_month.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "6" ]]; then
  header 6 "Task 5 - Review score statistics"
  cat output/task5_review_stats.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "7" ]]; then
  header 7 "Task 6 - Revenue 2024 by product category"
  cat output/task6_revenue_2024_by_category.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "8" ]]; then
  header 8 "Task 7 - Top-selling products and avg review score"
  cat output/task7_top_products.txt
  pause
fi

if [[ "$STEP" == "all" || "$STEP" == "9" ]]; then
  header 9 "Task 10 - Seller ranking by total revenue"
  cat output/task10_seller_ranking.txt
  pause
fi

echo ""
echo "Done. Save screenshots to: $ROOT/screenshots/"
