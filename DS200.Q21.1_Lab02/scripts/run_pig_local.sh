#!/usr/bin/env bash
# Run all Pig scripts in local mode. Requires `pig` on PATH.
# Writes pig.params.properties with absolute paths next to this script's lab root.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIG_DIR="$ROOT/pig"
PARAMS="$PIG_DIR/params.properties"

if ! command -v pig >/dev/null 2>&1; then
  echo "Apache Pig not found. Install Pig or use: python3 \"$ROOT/scripts/generate_outputs.py\""
  exit 1
fi

cat >"$PARAMS" <<EOF
INPUT_REVIEW=$ROOT/data/hotel-review.csv
INPUT_STOP=$ROOT/data/stopwords.txt
OUT_TASK1=$ROOT/output/pig_task01_tokens
OUT_TASK2A=$ROOT/output/pig_task02_wordfreq_gt500
OUT_TASK2B=$ROOT/output/pig_task02_comments_by_category
OUT_TASK2C=$ROOT/output/pig_task02_comments_by_aspect
OUT_TASK3A=$ROOT/output/pig_task03_aspect_negative_counts
OUT_TASK3B=$ROOT/output/pig_task03_aspect_positive_counts
OUT_TASK4A=$ROOT/output/pig_task04_top5_positive_by_category
OUT_TASK4B=$ROOT/output/pig_task04_top5_negative_by_category
OUT_TASK5=$ROOT/output/pig_task05_top5_words_by_category
EOF

echo "Running as user: $(whoami)"
echo "Lab root: $ROOT"

rm -rf \
  "$ROOT/output/pig_task01_tokens" \
  "$ROOT/output/pig_task02_wordfreq_gt500" \
  "$ROOT/output/pig_task02_comments_by_category" \
  "$ROOT/output/pig_task02_comments_by_aspect" \
  "$ROOT/output/pig_task03_aspect_negative_counts" \
  "$ROOT/output/pig_task03_aspect_positive_counts" \
  "$ROOT/output/pig_task04_top5_positive_by_category" \
  "$ROOT/output/pig_task04_top5_negative_by_category" \
  "$ROOT/output/pig_task05_top5_words_by_category"

pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task01_preprocess.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task02_statistics.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task03_aspect_sentiment.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task04_sentiment_words.pig"
pig -x local -param_file "$PARAMS" -f "$PIG_DIR/task05_category_words.pig"

echo "Pig outputs are under $ROOT/output/pig_* (part-r-* files)."
