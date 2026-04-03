# DS200 — Lab 02 (Apache Pig — hotel review text analytics)

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
| `scripts/generate_outputs.py` | Python mirror of the Pig logic; writes compact **TSV** files into `output/` when Pig is unavailable. |
| `output/*.tsv` | Committed result tables for submission (see below). |
| `screenshots/` | Add your own PNG/JPG captures; see `screenshots/SCREENSHOTS.txt`. |

## How to run

### Option A — Apache Pig (expected for the course)

From this directory:

```bash
bash scripts/run_pig_local.sh
```

This generates `output/pig_*` folders with `part-r-*` files. Inspect them with `cat`, `head`, or your editor.

`task04_sentiment_words.pig` and `task05_category_words.pig` use `FLATTEN(top.(word, cnt))` inside nested `FOREACH` blocks; that requires a reasonably recent Pig (0.14+). If your cluster rejects that syntax, replace it with `FLATTEN(top)` and drop duplicate columns when reading the output.

### Option B — Python (same outputs as TSV)

```bash
python3 scripts/generate_outputs.py
```

## Output files (TSV)

| File | Task |
|------|------|
| `output/task01_tokens.tsv` | Cleaned tokens with segment metadata (large, ~6–7 MB). |
| `output/task02_wordfreq_gt500.tsv` | Words with total count > 500. |
| `output/task02_comments_by_category.tsv` | Number of segments per category. |
| `output/task02_comments_by_aspect.tsv` | Number of segments per aspect. |
| `output/task03_aspect_negative_counts.tsv` | Negative segment counts per aspect (sorted). |
| `output/task03_aspect_positive_counts.tsv` | Positive segment counts per aspect (sorted). |
| `output/task03_summary.tsv` | Top aspect for negative / positive (quick answer). |
| `output/task04_top5_positive_words_by_category.tsv` | Top 5 frequent words in **positive** segments, per category. |
| `output/task04_top5_negative_words_by_category.tsv` | Top 5 frequent words in **negative** segments, per category. |
| `output/task05_top5_words_by_category.tsv` | Top 5 frequent words per category (all sentiments). |

## Task 3 answers (from `task03_summary.tsv`)

- **Most negative aspect:** `ROOMS` (515 negative segments in this dataset).
- **Most positive aspect:** `HOTEL` (2942 positive segments).

## Screenshots (grading / identity)

Place images under `screenshots/` that show **your username** (e.g. `whoami` in the same terminal pane) plus the command you ran and a preview of results. Follow `screenshots/SCREENSHOTS.txt`.

## Notes

- Tokenization follows the assignment: **split on whitespace** after lowercasing; common punctuation is turned into spaces so tokens are not glued to commas.
- Multi-word stop phrases (e.g. `chúng ta`) only match if the tokenizer emits the exact same token string (whitespace-delimited).
