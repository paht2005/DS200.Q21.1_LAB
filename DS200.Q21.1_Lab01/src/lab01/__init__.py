"""Public package surface: I/O loaders and task 1–4 analytics entry points."""

from lab01.analytics import (
    age_bucket,
    task1_average_ratings_per_movie,
    task2_average_ratings_per_genre,
    task3_ratings_by_gender_per_movie,
    task4_ratings_by_age_group_per_movie,
)
from lab01.io import get_data_dir, load_movies, load_ratings, load_users, project_root

__all__ = [
    "age_bucket",
    "get_data_dir",
    "load_movies",
    "load_ratings",
    "load_users",
    "project_root",
    "task1_average_ratings_per_movie",
    "task2_average_ratings_per_genre",
    "task3_ratings_by_gender_per_movie",
    "task4_ratings_by_age_group_per_movie",
]
