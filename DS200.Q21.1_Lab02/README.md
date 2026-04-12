# DS200 — Lab 02 (Apache Pig — hotel review text analytics)

<!-- Contact section: centered, single row of badges, short intro -->
<div align="center">


<!-- HEADER BANNER -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=0:667eea,100:764ba2&height=220&section=header&text=Nguyen%20Cong%20Phat%20(James)&fontSize=38&fontColor=ffffff&animation=fadeIn&fontAlignY=35&desc=AI%20Engineer%20%7C%20Data%20Scientist%20%7C%20Community%20Builder&descSize=16&descAlignY=55&descAlign=50" width="100%"/>

<br/>

[![Portfolio](https://img.shields.io/badge/Portfolio-667eea?style=for-the-badge&logo=vercel&logoColor=white)](https://nguyen-cong-phat-portfolio.vercel.app/)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ncp2005/)
[![Email](https://img.shields.io/badge/Email-EA4335?style=for-the-badge&logo=gmail&logoColor=white)](mailto:congphatnguyen.work@gmail.com)
[![Facebook](https://img.shields.io/badge/Facebook-1877F2?style=for-the-badge&logo=facebook&logoColor=white)](https://www.facebook.com/phat.nguyencong.2005/)

<br/>

> *"Everything is a win when the goal is experience."*


</div>



**Student:** Phat Cong Nguyen - 23521143

This folder matches the five tasks described in `assignments.ipynb`: preprocessing (lowercase, tokenize, stopwords), frequency and segment statistics, aspect–sentiment summaries, and top words per category / polarity.

## Table of contents

- [Prerequisites](#prerequisites)
- [Repository layout](#repository-layout)
- [How to run](#how-to-run)
- [Output files](#output-files-pig-part-r-00000)
- [Task 3 answers](#task-3-answers)
- [Screenshots](#screenshots)
- [Submission](#submission)
- [Notes](#notes)

## Prerequisites

### 1. Install Java (JDK 8)

Apache Pig 0.17 requires **Java 8**. Check if you already have it:

```bash
java -version
# Should show: java version "1.8.x" or openjdk version "1.8.x"
```

**macOS (Homebrew):**

```bash
brew install --cask temurin@8
```

**Ubuntu / Debian:**

```bash
sudo apt update && sudo apt install openjdk-8-jdk -y
```

Set `JAVA_HOME` if not set automatically:

```bash
# Add to ~/.zshrc or ~/.bashrc
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8 2>/dev/null || echo "/usr/lib/jvm/java-8-openjdk-amd64")
export PATH="$JAVA_HOME/bin:$PATH"
```

Reload your shell:

```bash
source ~/.zshrc   # or source ~/.bashrc
```

### 2. Install Apache Pig

**macOS (Homebrew):**

```bash
brew install pig
```

**Manual install (all platforms):**

```bash
# Download Pig 0.17.0
curl -O https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz
tar -xzf pig-0.17.0.tar.gz
sudo mv pig-0.17.0 /usr/local/pig
```

Add Pig to your `PATH`:

```bash
# Add to ~/.zshrc or ~/.bashrc
export PIG_HOME=/usr/local/pig
export PATH="$PIG_HOME/bin:$PATH"
```

Reload and verify:

```bash
source ~/.zshrc
pig --version
# Expected: Apache Pig version 0.17.0 (r1797386)
```

### 3. Verify the installation

```bash
whoami          # confirm your user
java -version   # JDK 8
pig --version   # Apache Pig 0.17.0
```

If all three commands produce output without errors, you are ready to run the scripts.

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

Current screenshot files:

| File | Description |
|------|-------------|
| `screenshots/00.1_run_local_pig_start.png` | Extra capture: start of the full local Pig run. |
| `screenshots/00.2_run_local_pig_end.png` | Extra capture: end of the full local Pig run. |
| `screenshots/01_whoami_and_run.png` | Screenshot 1: identity and Pig run command. |
| `screenshots/02_task01_tokens.png` | Screenshot 2: Task 1 output preview. |
| `screenshots/03.1_task02_wordfreq.png` | Screenshot 3 (part 1): Task 2 word frequency output. |
| `screenshots/03.2_task02_wordfreq.png` | Screenshot 3 (part 2): continued Task 2 word frequency output. |
| `screenshots/04_task02_category_aspect.png` | Screenshot 4: Task 2 category/aspect statistics. |
| `screenshots/05_task03_sentiment.png` | Screenshot 5: Task 3 aspect sentiment counts. |
| `screenshots/06_task04_negative.png` | Screenshot 7: Task 4 top 5 negative words. |
| `screenshots/07_task04_positive.png` | Screenshot 6: Task 4 top 5 positive words. |
| `screenshots/08_task05_top_words.png` | Screenshot 8: Task 5 top words by category. |

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
