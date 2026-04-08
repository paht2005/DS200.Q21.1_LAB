# DS200 — Lab 02 (Apache Pig — hotel review text analytics)

**Student:** Phat Cong Nguyen - 23521143

This folder matches the five tasks described in `assignments.ipynb`: preprocessing (lowercase, tokenize, stopwords), frequency and segment statistics, aspect–sentiment summaries, and top words per category / polarity.

## Repository layout

| Path | Purpose |
|------|---------|
| `assignments.ipynb` | Lab wording (Vietnamese). |
| `data/hotel-review.csv` | Semicolon-separated segments: `id;comment;category;aspect;sentiment`. |
| `data/stopwords.txt` | Stop words (one entry per line). |
| `pig/*.pig` | Apache Pig Latin sources (comments in English). |
| `pig/params.properties.example` | Template for Pig parameters (paths). |
| `scripts/run_pig_local.sh` | Runs all Pig scripts in local mode (requires `pig` on `PATH`). |
| `scripts/screenshots.sh` | Displays output results with student info for taking screenshots. |
| `output/pig_*/part-r-00000` | Pig result files (CRC and `_SUCCESS` markers are git-ignored). |
| `screenshots/` | Terminal screenshots with student identity and results. |

## How to run

From this directory:

```bash
bash scripts/run_pig_local.sh
```

This generates `output/pig_*` folders with `part-r-*` files. Inspect them with `cat`, `head`, or your editor.

## Output files (Pig `part-r-00000`)

| Directory | Task |
|-----------|------|
| `output/pig_task01_tokens/` | Cleaned tokens with segment metadata. |
| `output/pig_task02_wordfreq_gt500/` | Words with total count > 500. |
| `output/pig_task02_comments_by_category/` | Number of segments per category. |
| `output/pig_task02_comments_by_aspect/` | Number of segments per aspect. |
| `output/pig_task03_aspect_negative_counts/` | Negative segment counts per aspect (sorted). |
| `output/pig_task03_aspect_positive_counts/` | Positive segment counts per aspect (sorted). |
| `output/pig_task04_top5_positive_by_category/` | Top 5 frequent words in **positive** segments, per category. |
| `output/pig_task04_top5_negative_by_category/` | Top 5 frequent words in **negative** segments, per category. |
| `output/pig_task05_top5_words_by_category/` | Top 5 frequent words per category (all sentiments). |

## Task 3 answers

- **Most negative aspect:** `ROOMS` (515 negative segments).
- **Most positive aspect:** `HOTEL` (2942 positive segments).

## Screenshots

Screenshots under `screenshots/` show username (`whoami`), Pig run command, and result previews.

To reproduce the screenshots yourself:

```bash
# Run all at once (press Enter between each to take a screenshot)
bash scripts/screenshots.sh

# Or run a single screenshot (1–8)
bash scripts/screenshots.sh 1   # Identity & Pig Run
bash scripts/screenshots.sh 2   # Task 1 — Preprocessed Tokens
bash scripts/screenshots.sh 3   # Task 2 — Word Freq > 500
bash scripts/screenshots.sh 4   # Task 2 — Category & Aspect Stats
bash scripts/screenshots.sh 5   # Task 3 — Aspect Sentiment
bash scripts/screenshots.sh 6   # Task 4 — Top 5 Positive Words
bash scripts/screenshots.sh 7   # Task 4 — Top 5 Negative Words
bash scripts/screenshots.sh 8   # Task 5 — Top 5 Words by Category
```

## Submission

Submit via GitHub. The repo includes:
- Apache Pig source files (`pig/*.pig`)
- Result files (`output/pig_*/part-r-00000`)
- Terminal screenshots (`screenshots/*.png`)

## Notes

- Pig's `REPLACE()` uses Java regex — `.` and `?` must be escaped as `\\.` and `\\?` in Pig Latin strings.
- Tokenization follows the assignment: **split on whitespace** after lowercasing; common punctuation is turned into spaces.
- Multi-word stop phrases (e.g. `chúng ta`) only match if the tokenizer emits the exact same token string.
