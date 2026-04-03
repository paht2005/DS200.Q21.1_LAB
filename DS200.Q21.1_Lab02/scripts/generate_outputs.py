#!/usr/bin/env python3
"""
Mirror Lab 02 Pig logic in plain Python so result TSV files can be produced
when Apache Pig is not installed. Logic matches pig/*.pig (whitespace tokens,
lowercase, punctuation-to-space, exact stopword match).

Run from anywhere:
  python3 DS200.Q21.1_Lab02/scripts/generate_outputs.py
"""

from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from pathlib import Path


def lab_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_stopwords(path: Path) -> set[str]:
    stops: set[str] = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            w = line.strip().lower()
            if w:
                stops.add(w)
    return stops


def normalize_comment(text: str) -> str:
    """Lowercase and replace common punctuation with spaces (same idea as Pig REPLACE chain)."""
    t = text.strip().lower()
    for ch in ",.!?;":
        t = t.replace(ch, " ")
    return t


def tokenize(text: str) -> list[str]:
    return [w for w in normalize_comment(text).split() if w]


def load_segments(review_path: Path, stops: set[str]) -> tuple[list[dict], list[dict]]:
    """
    Returns:
      raw_segments: one dict per CSV row (segment)
      token_rows: one dict per (segment, token) after stopword removal
    """
    raw_segments: list[dict] = []
    token_rows: list[dict] = []
    with review_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            parts = line.split(";")
            if len(parts) < 5:
                continue
            rid, comment, category, aspect, sentiment = (
                parts[0],
                parts[1],
                parts[2],
                parts[3],
                parts[4],
            )
            row = {
                "review_id": rid,
                "category": category,
                "aspect": aspect,
                "sentiment": sentiment.strip(),
            }
            raw_segments.append(row)
            for w in tokenize(comment):
                if w not in stops:
                    token_rows.append({**row, "word": w})
    return raw_segments, token_rows


def write_tsv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(header)
        w.writerows(rows)


def main() -> None:
    root = lab_root()
    data = root / "data"
    review_path = data / "hotel-review.csv"
    stop_path = data / "stopwords.txt"
    out_dir = root / "output"

    stops = load_stopwords(stop_path)
    raw_segments, token_rows = load_segments(review_path, stops)

    # Task 1: one row per cleaned token with segment metadata
    write_tsv(
        out_dir / "task01_tokens.tsv",
        ["review_id", "category", "aspect", "sentiment", "word"],
        [[r["review_id"], r["category"], r["aspect"], r["sentiment"], r["word"]] for r in token_rows],
    )

    # Task 2a: word counts > 500
    wc = Counter(r["word"] for r in token_rows)
    wf = sorted(((w, c) for w, c in wc.items() if c > 500), key=lambda x: (-x[1], x[0]))
    write_tsv(out_dir / "task02_wordfreq_gt500.tsv", ["word", "count"], wf)

    # Task 2b / 2c: segment counts
    by_cat = Counter(r["category"] for r in raw_segments)
    by_asp = Counter(r["aspect"] for r in raw_segments)
    write_tsv(
        out_dir / "task02_comments_by_category.tsv",
        ["category", "count"],
        sorted(by_cat.items(), key=lambda x: (-x[1], x[0])),
    )
    write_tsv(
        out_dir / "task02_comments_by_aspect.tsv",
        ["aspect", "count"],
        sorted(by_asp.items(), key=lambda x: (-x[1], x[0])),
    )

    # Task 3: aspect x sentiment counts
    neg_asp = Counter(r["aspect"] for r in raw_segments if r["sentiment"] == "negative")
    pos_asp = Counter(r["aspect"] for r in raw_segments if r["sentiment"] == "positive")
    neg_sorted = sorted(neg_asp.items(), key=lambda x: (-x[1], x[0]))
    pos_sorted = sorted(pos_asp.items(), key=lambda x: (-x[1], x[0]))
    write_tsv(out_dir / "task03_aspect_negative_counts.tsv", ["aspect", "count"], neg_sorted)
    write_tsv(out_dir / "task03_aspect_positive_counts.tsv", ["aspect", "count"], pos_sorted)

    top_neg = neg_sorted[0] if neg_sorted else ("", 0)
    top_pos = pos_sorted[0] if pos_sorted else ("", 0)
    write_tsv(
        out_dir / "task03_summary.tsv",
        ["polarity", "aspect", "count"],
        [
            ["negative", top_neg[0], top_neg[1]],
            ["positive", top_pos[0], top_pos[1]],
        ],
    )

    def top5_words_per_category(
        rows: list[dict], pred
    ) -> list[tuple[str, str, int]]:
        """Return flat rows: category, word, count for top 5 per category."""
        filtered = [r for r in rows if pred(r)]
        # count (category, word)
        cc: Counter[tuple[str, str]] = Counter()
        for r in filtered:
            cc[(r["category"], r["word"])] += 1
        by_cat: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for (cat, word), c in cc.items():
            by_cat[cat].append((word, c))
        out: list[tuple[str, str, int]] = []
        for cat in sorted(by_cat.keys()):
            words = sorted(by_cat[cat], key=lambda x: (-x[1], x[0]))[:5]
            for word, c in words:
                out.append((cat, word, c))
        return out

    pos_top = top5_words_per_category(token_rows, lambda r: r["sentiment"] == "positive")
    neg_top = top5_words_per_category(token_rows, lambda r: r["sentiment"] == "negative")
    write_tsv(out_dir / "task04_top5_positive_words_by_category.tsv", ["category", "word", "count"], pos_top)
    write_tsv(out_dir / "task04_top5_negative_words_by_category.tsv", ["category", "word", "count"], neg_top)

    # Task 5: top 5 overall per category (all sentiments)
    all_top = top5_words_per_category(token_rows, lambda r: True)
    write_tsv(out_dir / "task05_top5_words_by_category.tsv", ["category", "word", "count"], all_top)

    print(f"Wrote TSV outputs under: {out_dir}")
    print(f"Host user (for screenshots): {os.environ.get('USER', os.environ.get('USERNAME', 'unknown'))}")


if __name__ == "__main__":
    main()
