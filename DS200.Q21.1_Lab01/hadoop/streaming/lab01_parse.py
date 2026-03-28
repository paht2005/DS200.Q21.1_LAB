# Shared CSV parsing for Lab 01 Hadoop Streaming jobs (comma-separated lab export).
# Ship this file next to mappers/reducers using Hadoop Streaming -files (or PYTHONPATH for local pipes).

from __future__ import annotations


def parse_movie_line(line: str) -> tuple[str, str, str] | None:
    """Return (MovieID, Title, Genres) or None if the line is invalid."""
    line = line.strip()
    if not line:
        return None
    parts = [p.strip() for p in line.split(",", 2)]
    if len(parts) < 3:
        return None
    return parts[0], parts[1], parts[2]


def parse_rating_line(line: str) -> tuple[str, str, float] | None:
    """Return (UserID, MovieID, Rating) or None."""
    line = line.strip()
    if not line:
        return None
    parts = [p.strip() for p in line.split(",", 3)]
    if len(parts) < 3:
        return None
    try:
        rating = float(parts[2])
    except ValueError:
        return None
    return parts[0], parts[1], rating


def parse_user_line(line: str) -> tuple[str, str, int] | None:
    """Return (UserID, Gender, Age) or None."""
    line = line.strip()
    if not line:
        return None
    parts = [p.strip() for p in line.split(",", 4)]
    if len(parts) < 3:
        return None
    try:
        age = int(parts[2])
    except ValueError:
        return None
    return parts[0], parts[1].upper(), age


def load_movies_from_path(path: str) -> dict[str, str]:
    """Build MovieID -> Title from movies.txt."""
    out: dict[str, str] = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parsed = parse_movie_line(line)
            if parsed:
                mid, title, _ = parsed
                out[mid] = title
    return out


def load_users_from_path(path: str) -> dict[str, tuple[str, int]]:
    """Build UserID -> (Gender, Age) from users.txt."""
    out: dict[str, tuple[str, int]] = {}
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parsed = parse_user_line(line)
            if parsed:
                uid, gender, age = parsed
                out[uid] = (gender, age)
    return out


def age_bucket(age: int) -> str:
    """Disjoint age buckets required by the assignment (see README)."""
    if age <= 18:
        return "0-18"
    if age <= 35:
        return "18-35"
    if age <= 50:
        return "35-50"
    return "50+"


def fmt_rating(x: float) -> str:
    """Trim trailing zeros for report lines."""
    s = f"{x:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"
