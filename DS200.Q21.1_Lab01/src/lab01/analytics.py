"""Vectorized analytics for Lab 01 exercises (tasks 1–4).

Implements the same aggregates as the MapReduce-style assignment brief
(average ratings, joins, pivots) using pandas for local execution.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lab01.io import load_movies, load_ratings, load_users


def _fmt_rating(x: float) -> str:
    """Trim trailing zeros from a float string for readable report lines."""
    s = f"{x:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


def age_bucket(age: int) -> str:
    """Map a numeric age to one of four disjoint buckets used in task 4.

    Buckets: 0–18 (inclusive), 19–35 labeled ``18-35``, 36–50 labeled ``35-50``,
    51+ labeled ``50+`` (see lab README for boundary notes).
    """
    if age <= 18:
        return "0-18"
    if age <= 35:
        return "18-35"
    if age <= 50:
        return "35-50"
    return "50+"


def task1_average_ratings_per_movie(
    data_dir: Path | None = None,
) -> tuple[str, pd.DataFrame]:
    """Task 1: mean rating and count per movie; highlight best movie with ≥5 ratings."""
    movies = load_movies(data_dir)
    # Concatenated ratings_1 + ratings_2 are loaded inside load_ratings()
    ratings = load_ratings(data_dir)

    # Aggregate in one pass: sum would need a second step; mean + count matches the spec
    g = ratings.groupby("MovieID", as_index=False).agg(
        AverageRating=("Rating", "mean"),
        TotalRatings=("Rating", "count"),
    )
    merged = g.merge(movies, on="MovieID", how="left")

    lines: list[str] = []
    for _, row in merged.sort_values("Title").iterrows():
        title = str(row["Title"])
        avg = float(row["AverageRating"])
        cnt = int(row["TotalRatings"])
        lines.append(
            f"{title} AverageRating: {_fmt_rating(avg)} (TotalRatings: {cnt})"
        )

    # Eligibility rule from assignment: only movies with at least five ratings
    eligible = merged[merged["TotalRatings"] >= 5]
    if eligible.empty:
        summary = "No movie has at least 5 ratings."
    else:
        idx = eligible["AverageRating"].idxmax()
        best = eligible.loc[idx]
        summary = (
            f"{best['Title']} is the highest rated movie with an average rating of "
            f"{_fmt_rating(float(best['AverageRating']))} among movies with at least 5 ratings."
        )
    return "\n".join(lines + ["", summary]), merged


def task2_average_ratings_per_genre(
    data_dir: Path | None = None,
) -> tuple[str, pd.DataFrame]:
    """Task 2: split pipe-separated genres, then average rating per genre."""
    movies = load_movies(data_dir)
    ratings = load_ratings(data_dir)

    # One row per (MovieID, single genre) after explode — mirrors emitting one KV per genre in MR
    m = movies.assign(Genre=movies["Genres"].str.split("|")).explode("Genre")
    m = m.dropna(subset=["Genre"])
    m["Genre"] = m["Genre"].str.strip()
    r = ratings.merge(m[["MovieID", "Genre"]], on="MovieID", how="inner")

    g = r.groupby("Genre", as_index=False).agg(
        AverageRating=("Rating", "mean"),
        TotalRatings=("Rating", "count"),
    )
    lines: list[str] = []
    for _, row in g.sort_values("Genre").iterrows():
        lines.append(
            f"{row['Genre']}: {_fmt_rating(float(row['AverageRating']))} "
            f"(TotalRatings: {int(row['TotalRatings'])})"
        )
    return "\n".join(lines), g


def task3_ratings_by_gender_per_movie(
    data_dir: Path | None = None,
) -> tuple[str, pd.DataFrame]:
    """Task 3: join ratings with users, then mean rating per movie by gender (M/F)."""
    movies = load_movies(data_dir)
    ratings = load_ratings(data_dir)
    users = load_users(data_dir)

    ru = ratings.merge(users[["UserID", "Gender"]], on="UserID", how="inner")
    ru = ru.merge(movies[["MovieID", "Title"]], on="MovieID", how="left")

    pivot = (
        ru.pivot_table(
            index="Title",
            columns="Gender",
            values="Rating",
            aggfunc="mean",
        )
        .rename(columns={"M": "Male_Avg", "F": "Female_Avg"})
    )
    # Ensure both columns exist so output format stays stable when one gender is missing
    for col in ("Male_Avg", "Female_Avg"):
        if col not in pivot.columns:
            pivot[col] = pd.NA
    pivot = pivot.sort_index()

    lines: list[str] = []
    for title, row in pivot.iterrows():
        male = row.get("Male_Avg")
        female = row.get("Female_Avg")
        m_s = "N/A" if pd.isna(male) else _fmt_rating(float(male))
        f_s = "N/A" if pd.isna(female) else _fmt_rating(float(female))
        lines.append(f"{title}: {m_s}, {f_s}")
    return "\n".join(lines), pivot.reset_index()


def task4_ratings_by_age_group_per_movie(
    data_dir: Path | None = None,
) -> tuple[str, pd.DataFrame]:
    """Task 4: bucket user age, then mean rating per movie per age group."""
    movies = load_movies(data_dir)
    ratings = load_ratings(data_dir)
    users = load_users(data_dir)

    ru = ratings.merge(users[["UserID", "Age"]], on="UserID", how="inner")
    ru = ru.merge(movies[["MovieID", "Title"]], on="MovieID", how="left")
    ru["AgeGroup"] = ru["Age"].map(age_bucket)

    order = ["0-18", "18-35", "35-50", "50+"]
    pivot = (
        ru.pivot_table(
            index="Title",
            columns="AgeGroup",
            values="Rating",
            aggfunc="mean",
        )
        .reindex(columns=order)
    )
    pivot = pivot.sort_index()

    lines: list[str] = []
    for title, row in pivot.iterrows():
        parts: list[str] = []
        for bucket in order:
            v = row.get(bucket)
            parts.append(
                f"{bucket}: {'N/A' if pd.isna(v) else _fmt_rating(float(v))}"
            )
        lines.append(f"{title}: [{', '.join(parts)}]")
    return "\n".join(lines), pivot.reset_index()
