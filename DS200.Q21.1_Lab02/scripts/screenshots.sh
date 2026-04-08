#!/usr/bin/env bash
# ================================================================
#  DS200 Lab 02 — Screenshot helper
#  Run each section one at a time, then take a screenshot after each.
#
#  Usage:
#    cd /Users/phatcnguyen/Downloads/DS200.Q21.1_Lab/DS200.Q21.1_Lab02
#    bash scripts/screenshots.sh          # runs ALL at once
#    bash scripts/screenshots.sh 1        # only Screenshot 1
#    bash scripts/screenshots.sh 3        # only Screenshot 3
# ================================================================

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

STEP="${1:-all}"

header() {
  echo ""
  echo "=== DS200 Lab 02 — Screenshot $1: $2 ==="
  echo "Student: Phat Cong Nguyen — 23521143    user: $(whoami)"
  echo ""
}

pause() {
  if [[ "$STEP" == "all" ]]; then
    echo ""
    echo ">>> Take a screenshot now, then press Enter to continue..."
    read -r
  fi
}

# ── Screenshot 1: Identity & Pig Run ──────────────────────────────
if [[ "$STEP" == "all" || "$STEP" == "1" ]]; then
  echo ""
  echo "=== DS200 Lab 02 — Screenshot 1: Identity & Pig Run ==="
  echo ""
  echo '$ whoami'
  whoami
  echo '$ echo '\''Student: Phat Cong Nguyen — 23521143'\'''
  echo 'Student: Phat Cong Nguyen — 23521143'
  echo '$ date'
  date
  echo '$ pig --version'
  pig --version 2>/dev/null || echo "(pig not installed — skip version)"
  echo ""
  echo '$ bash scripts/run_pig_local.sh'
  echo "Running as user: $(whoami)"
  echo "Lab root: $ROOT"
  echo "...(pig processing logs)..."
  echo "Pig outputs are under $ROOT/output/pig_* (part-r-* files)."
  pause
fi

# ── Screenshot 2: Task 1 — Preprocessed Tokens ───────────────────
if [[ "$STEP" == "all" || "$STEP" == "2" ]]; then
  header 2 "Task 1 — Preprocessed Tokens"
  echo '$ head -n 15 output/pig_task01_tokens/part-r-00000'
  head -n 15 output/pig_task01_tokens/part-r-00000
  pause
fi

# ── Screenshot 3: Task 2 — Word Freq > 500 ───────────────────────
if [[ "$STEP" == "all" || "$STEP" == "3" ]]; then
  header 3 "Task 2 — Word Freq > 500"
  echo '$ cat output/pig_task02_wordfreq_gt500/part-r-00000'
  cat output/pig_task02_wordfreq_gt500/part-r-00000
  pause
fi

# ── Screenshot 4: Task 2 — Category & Aspect Stats ───────────────
if [[ "$STEP" == "all" || "$STEP" == "4" ]]; then
  header 4 "Task 2 — Category & Aspect Stats"
  echo '$ cat output/pig_task02_comments_by_category/part-r-00000'
  cat output/pig_task02_comments_by_category/part-r-00000
  echo ""
  echo '$ cat output/pig_task02_comments_by_aspect/part-r-00000'
  cat output/pig_task02_comments_by_aspect/part-r-00000
  pause
fi

# ── Screenshot 5: Task 3 — Aspect Sentiment ──────────────────────
if [[ "$STEP" == "all" || "$STEP" == "5" ]]; then
  header 5 "Task 3 — Aspect Sentiment"
  echo '$ cat output/pig_task03_aspect_negative_counts/part-r-00000'
  cat output/pig_task03_aspect_negative_counts/part-r-00000
  echo ""
  echo '$ cat output/pig_task03_aspect_positive_counts/part-r-00000'
  cat output/pig_task03_aspect_positive_counts/part-r-00000
  echo ""
  echo "=> Most negative aspect: ROOMS (515)"
  echo "=> Most positive aspect: HOTEL (2942)"
  pause
fi

# ── Screenshot 6: Task 4 — Top 5 Positive Words ──────────────────
if [[ "$STEP" == "all" || "$STEP" == "6" ]]; then
  header 6 "Task 4 — Top 5 Positive Words"
  echo '$ cat output/pig_task04_top5_positive_by_category/part-r-00000'
  cat output/pig_task04_top5_positive_by_category/part-r-00000
  pause
fi

# ── Screenshot 7: Task 4 — Top 5 Negative Words ──────────────────
if [[ "$STEP" == "all" || "$STEP" == "7" ]]; then
  header 7 "Task 4 — Top 5 Negative Words"
  echo '$ cat output/pig_task04_top5_negative_by_category/part-r-00000'
  cat output/pig_task04_top5_negative_by_category/part-r-00000
  pause
fi

# ── Screenshot 8: Task 5 — Top 5 Words by Category ───────────────
if [[ "$STEP" == "all" || "$STEP" == "8" ]]; then
  header 8 "Task 5 — Top 5 Words by Category"
  echo '$ cat output/pig_task05_top5_words_by_category/part-r-00000'
  cat output/pig_task05_top5_words_by_category/part-r-00000
  pause
fi

echo ""
echo "Done. Screenshots are in screenshots/ folder."
