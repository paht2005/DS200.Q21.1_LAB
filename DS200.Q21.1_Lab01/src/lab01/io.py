"""Load comma-separated movie, rating, and user tables from the lab dataset."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

# MovieLens-style exports often use Latin-1; keeps titles with accents intact
ENCODING = "ISO-8859-1"
_ENV_DATA_DIR = "LAB01_DATA_DIR"


def project_root() -> Path:
    """Return the lab repository root (parent of ``src/lab01``)."""
    return Path(__file__).resolve().parents[2]


def default_data_dir() -> Path:
    """Default to ``<project_root>/data`` unless ``LAB01_DATA_DIR`` is set."""
    env = os.environ.get(_ENV_DATA_DIR)
    if env:
        return Path(env).expanduser().resolve()
    return project_root() / "data"


def get_data_dir(override: Path | None = None) -> Path:
    """Resolve the folder that contains ``movies.txt`` and rating files."""
    if override is not None:
        return override.resolve()
    return default_data_dir()


def load_movies(data_dir: Path | None = None) -> pd.DataFrame:
    """Read ``movies.txt``: MovieID, Title, Genres (genres may contain ``|``)."""
    base = get_data_dir(data_dir)
    path = base / "movies.txt"
    return pd.read_csv(
        path,
        header=None,
        names=["MovieID", "Title", "Genres"],
        encoding=ENCODING,
        skipinitialspace=True,
    )


def load_ratings(data_dir: Path | None = None) -> pd.DataFrame:
    """Read and concatenate ``ratings_1.txt`` and ``ratings_2.txt`` (same schema)."""
    base = get_data_dir(data_dir)
    parts = [
        pd.read_csv(
            base / "ratings_1.txt",
            header=None,
            names=["UserID", "MovieID", "Rating", "Timestamp"],
            encoding=ENCODING,
            skipinitialspace=True,
        ),
        pd.read_csv(
            base / "ratings_2.txt",
            header=None,
            names=["UserID", "MovieID", "Rating", "Timestamp"],
            encoding=ENCODING,
            skipinitialspace=True,
        ),
    ]
    return pd.concat(parts, ignore_index=True)


def load_users(data_dir: Path | None = None) -> pd.DataFrame:
    """Read ``users.txt`` for joins (gender / age) in tasks 3 and 4."""
    base = get_data_dir(data_dir)
    path = base / "users.txt"
    return pd.read_csv(
        path,
        header=None,
        names=["UserID", "Gender", "Age", "Occupation", "Zip-code"],
        encoding=ENCODING,
        skipinitialspace=True,
    )
