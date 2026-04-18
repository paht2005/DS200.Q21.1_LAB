<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 03)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-11+-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 11+" />
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 3.5.x" />
  <img src="https://img.shields.io/badge/RDD-Java-blue?style=for-the-badge" alt="Java RDD" />
</p>

<p align="center">
  <a href="https://github.com/paht2005"><img src="https://img.shields.io/badge/GitHub-paht2005-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/"><img src="https://img.shields.io/badge/LinkedIn-Phat%20Nguyen-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23521143@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23521143%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab/DS200.Q21.1_Lab03/` — Parent overview: [../README.md](../README.md)

---

## Student information

| Student ID | Full name        | GitHub                                  | Email                  |
|:----------:|------------------|-----------------------------------------|------------------------|
| 23521143   | Phat Cong Nguyen | [paht2005](https://github.com/paht2005) | 23521143@gm.uit.edu.vn |

---

## Outline

1. [Objective](#objective)
2. [Repository layout](#repository-layout)
3. [Dataset](#dataset)
4. [Assignments implemented](#assignments-implemented)
5. [Which code is main](#which-code-is-main)
6. [End-to-end data flow](#end-to-end-data-flow)
7. [Task-by-task details](#task-by-task-details)
8. [Prerequisites](#prerequisites)
9. [Run all tasks](#run-all-tasks)
10. [Run each task individually](#run-each-task-individually)
11. [How to capture screenshots](#how-to-capture-screenshots)
12. [Output files](#output-files)
13. [Key classes reference](#key-classes-reference)
14. [Implementation notes](#implementation-notes)
15. [Submission checklist](#submission-checklist)

---

## Objective

Implement all six assignments from `assignments.ipynb` using **Java Spark RDD** (no DataFrames, no Python). Each task reads comma-delimited text files, processes them through RDD transformations, and writes a plain-text report.

---

## Repository layout

```text
DS200.Q21.1_Lab03/
├── README.md
├── assignments.ipynb               <- Assignment descriptions (6 tasks)
├── data/
│   ├── movies.txt                  <- MovieID, Title, Genres
│   ├── ratings_1.txt               <- UserID, MovieID, Rating, Timestamp
│   ├── ratings_2.txt               <- UserID, MovieID, Rating, Timestamp
│   ├── users.txt                   <- UserID, Gender, Age, OccupationID, Zip
│   └── occupation.txt              <- ID, Occupation
├── scripts/
│   ├── run_java_rdd_local.sh       <- Main script: build JAR + run all 6 tasks
│   └── java.sh                     <- Convenience wrapper
├── spark/
│   └── java/
│       └── lab03-rdd/
│           ├── pom.xml             <- Maven config (Spark 3.5.x, Java 11)
│           └── src/main/java/ds200/lab03/
│               ├── model/          <- Movie, Rating, User, RatingStats
│               ├── util/           <- Lab03Parse, SparkContexts, OutputWriter
│               ├── task1/          <- Task1App  (avg rating per movie)
│               ├── task2/          <- Task2App  (avg rating per genre)
│               ├── task3/          <- Task3App  (avg rating per gender)
│               ├── task4/          <- Task4App  (avg rating per age group)
│               ├── task5/          <- Task5App  (avg rating per occupation)
│               └── task6/          <- Task6App  (avg rating per year)
└── output/                         <- Generated reports (6 text files)
```

---

## Dataset

All files use **comma (`,`)** as the delimiter.

| File | Schema | Rows | Description |
|------|--------|------|-------------|
| `data/movies.txt` | `MovieID,Title,Genres` | ~50 | Genres are pipe-separated (e.g. `Crime\|Drama`) |
| `data/ratings_1.txt` | `UserID,MovieID,Rating,Timestamp` | ~50 | Rating is a float (1.0–5.0), Timestamp is Unix epoch |
| `data/ratings_2.txt` | `UserID,MovieID,Rating,Timestamp` | ~50 | Second ratings file, union-ed with ratings_1 |
| `data/users.txt` | `UserID,Gender,Age,OccupationID,Zip` | ~50 | Gender: `M`/`F`, Age: integer |
| `data/occupation.txt` | `ID,Occupation` | 15 | Lookup table mapping OccupationID → name |

---

## Assignments implemented

| Task | Goal | Java class | Output |
|------|------|------------|--------|
| 1 | Average rating + count per movie; top movie (≥5 ratings) | `Task1App` | `task1_movie_ratings.txt` |
| 2 | Average rating by genre | `Task2App` | `task2_genre_ratings.txt` |
| 3 | Average rating by gender per movie | `Task3App` | `task3_gender_by_movie.txt` |
| 4 | Average rating by age group per movie | `Task4App` | `task4_age_groups_by_movie.txt` |
| 5 | Average rating + count by occupation | `Task5App` | `task5_occupation_ratings.txt` |
| 6 | Average rating + count by year | `Task6App` | `task6_yearly_ratings.txt` |

---

## Which code is main

| Layer | Role | Location |
|-------|------|----------|
| **Model** | Immutable domain objects (`Movie`, `Rating`, `User`, `RatingStats`) | `model/` |
| **Util** | CSV parsing, Spark context factory, file writer | `util/` |
| **Task Apps** | One `main()` per task — reads data, runs RDD pipeline, writes output | `task1/` … `task6/` |
| **Scripts** | Build JAR with Maven, run all tasks via `spark-submit` | `scripts/` |

---

## End-to-end data flow

```
┌────────────────────────────────────────────────────────────────────────┐
│  data/*.txt  ──►  sc.textFile()  ──►  parse + union  ──►  RDD        │
│                                                                        │
│  Small lookups (movies, users, occupations):                           │
│    sc.textFile() ──► parse ──► .collectAsMap() on driver               │
│                                                                        │
│  Ratings RDD:                                                          │
│    ratings_1 ∪ ratings_2  ──►  mapToPair(key, RatingStats)             │
│                           ──►  reduceByKey(RatingStats::merge)         │
│                           ──►  collect() on driver                     │
│                           ──►  sort + format                           │
│                           ──►  OutputWriter.write(output/*.txt)        │
└────────────────────────────────────────────────────────────────────────┘
```

**Mental model:** lookup maps are broadcast-like (collected to driver), ratings are the large RDD that gets aggregated via `reduceByKey`.

---

## Task-by-task details

### Task 1 — Average rating per movie

1. Collect `movies.txt` → `Map<Integer, String>` (movieId → title) on driver.
2. Union `ratings_1.txt` + `ratings_2.txt` → `JavaRDD<Rating>`.
3. `mapToPair(movieId, RatingStats(rating, 1))` → `reduceByKey(RatingStats::merge)`.
4. Sort by title alphabetically; output `MovieID|Title|AvgRating|Count`.
5. Filter movies with ≥5 ratings, find the one with the highest average (ties broken by count).
6. Append a `TOP_MOVIE(minRatings=5)` line.

### Task 2 — Average rating by genre

1. Collect `movies.txt` → `Map<Integer, List<String>>` (movieId → genres list).
2. `flatMapToPair`: for each rating, emit `(genre, RatingStats)` for **every** genre of that movie.
3. `reduceByKey` → sort by genre name → output `Genre|AvgRating|Count`.

### Task 3 — Average rating by gender per movie

1. Collect `users.txt` → `Map<Integer, String>` (userId → gender).
2. Collect `movies.txt` → movie title map.
3. `flatMapToPair`: emit composite key `movieId|gender` → `RatingStats`.
4. `reduceByKey`, then reconstruct per-movie gender stats on the driver.
5. Sort by title → output `MovieID|Title|MaleAvg|MaleCount|FemaleAvg|FemaleCount`.

### Task 4 — Average rating by age group per movie

1. Collect `users.txt` → `Map<Integer, String>` (userId → age group via `Lab03Parse.ageGroup()`).
2. Age groups: **0-18**, **19-35**, **36-50**, **51+**.
3. Composite key `movieId|ageGroup` → `reduceByKey` → sort by title then age group order.
4. Output `MovieID|Title|AgeGroup|AvgRating|Count`.

### Task 5 — Average rating by occupation

1. Collect `users.txt` → userId → occupationId; collect `occupation.txt` → id → name.
2. Key by occupation name → `reduceByKey(RatingStats::merge)`.
3. Sort alphabetically → output `Occupation|AvgRating|Count`.

### Task 6 — Average rating by year

1. Union both rating files into a single RDD.
2. Convert Unix timestamp → year via `Lab03Parse.yearFromTimestamp()` (UTC).
3. `mapToPair(year, RatingStats)` → `reduceByKey` → sort by year ascending.
4. Output `Year|AvgRating|Count`.

---

## Prerequisites

| Tool | Minimum version | Check command |
|------|-----------------|---------------|
| Java (JDK) | 11+ | `java -version` |
| Maven | 3.8+ | `mvn -version` |
| Apache Spark | 3.5.x | `spark-submit --version` |

`spark-submit` and `mvn` must be on your `PATH`.

---

## Run all tasks

From the `DS200.Q21.1_Lab03` directory:

```bash
bash scripts/run_java_rdd_local.sh
```

Or the convenience shortcut:

```bash
bash scripts/java.sh
```

**What the script does:**

1. Builds the Maven project at `spark/java/lab03-rdd` (`mvn package -DskipTests`).
2. Runs all 6 tasks via `spark-submit --master local[*]` with the built JAR.
3. Writes reports to `output/`.

---

## Run each task individually

From the `DS200.Q21.1_Lab03` directory:

```bash
cd spark/java/lab03-rdd
mvn -q package -DskipTests
cd ../../..
```

```bash
JAR="spark/java/lab03-rdd/target/lab03-rdd-1.0.0.jar"
DATA="data"
OUT="output"
mkdir -p "$OUT"
```

### Task 1

```bash
spark-submit --master local[*] --class ds200.lab03.task1.Task1App "$JAR" \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task1_movie_ratings.txt" "5"
```

### Task 2

```bash
spark-submit --master local[*] --class ds200.lab03.task2.Task2App "$JAR" \
  "$DATA/movies.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task2_genre_ratings.txt"
```

### Task 3

```bash
spark-submit --master local[*] --class ds200.lab03.task3.Task3App "$JAR" \
  "$DATA/movies.txt" "$DATA/users.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task3_gender_by_movie.txt"
```

### Task 4

```bash
spark-submit --master local[*] --class ds200.lab03.task4.Task4App "$JAR" \
  "$DATA/movies.txt" "$DATA/users.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task4_age_groups_by_movie.txt"
```

### Task 5

```bash
spark-submit --master local[*] --class ds200.lab03.task5.Task5App "$JAR" \
  "$DATA/users.txt" "$DATA/occupation.txt" "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task5_occupation_ratings.txt"
```

### Task 6

```bash
spark-submit --master local[*] --class ds200.lab03.task6.Task6App "$JAR" \
  "$DATA/ratings_1.txt" "$DATA/ratings_2.txt" \
  "$OUT/task6_yearly_ratings.txt"
```

---

## How to capture screenshots

Recommended workflow (as requested): run automatically, capture manually.

1. Run all tasks once:

```bash
bash scripts/run_java_rdd_local.sh
```

2. Create screenshot folder:

```bash
mkdir -p screenshots
```

3. Open each output in terminal and keep one clear view for capture:

```bash
cat output/task1_movie_ratings.txt
cat output/task2_genre_ratings.txt
cat output/task3_gender_by_movie.txt
cat output/task4_age_groups_by_movie.txt
cat output/task5_occupation_ratings.txt
cat output/task6_yearly_ratings.txt
```

4. Capture manually on macOS for each task:
- Press `Cmd + Shift + 4`, drag region around the terminal output.
- Save each image into `screenshots/`.

5. Suggested screenshot filenames:
- `screenshots/task1_movie_ratings.png`
- `screenshots/task2_genre_ratings.png`
- `screenshots/task3_gender_by_movie.png`
- `screenshots/task4_age_groups_by_movie.png`
- `screenshots/task5_occupation_ratings.png`
- `screenshots/task6_yearly_ratings.png`

---

## Output files

After a successful run:

| File | Content |
|------|---------|
| `output/task1_movie_ratings.txt` | Per-movie average + count, plus top movie line |
| `output/task2_genre_ratings.txt` | Per-genre average + count |
| `output/task3_gender_by_movie.txt` | Per-movie male/female average + count |
| `output/task4_age_groups_by_movie.txt` | Per-movie per-age-group average + count |
| `output/task5_occupation_ratings.txt` | Per-occupation average + count |
| `output/task6_yearly_ratings.txt` | Per-year average + count |

---

## Key classes reference

| Class | Package | Purpose |
|-------|---------|---------|
| `Movie` | `model` | Immutable record: movieId, title, genres (list) |
| `Rating` | `model` | Immutable record: userId, movieId, rating, timestamp |
| `User` | `model` | Immutable record: userId, gender, age, occupationId |
| `RatingStats` | `model` | Accumulator with `sum`, `count`, `getAverage()`, `merge()` |
| `Lab03Parse` | `util` | Parsing helpers: `parseMovie()`, `parseRating()`, `parseUser()`, `parseOccupation()`, `ageGroup()`, `yearFromTimestamp()`, `fmt()` |
| `SparkContexts` | `util` | Factory creating `JavaSparkContext` with `local[*]`, UI disabled |
| `OutputWriter` | `util` | Writes `List<String>` to a file, creating parent directories |

---

## Implementation notes

- All tasks use **RDD** exclusively (no DataFrame / Dataset / SparkSQL).
- Task 1 top-movie filter currently uses **minimum 5 ratings** in `scripts/run_java_rdd_local.sh`.
- Ratings input is the **union** of `ratings_1.txt` and `ratings_2.txt`.
- Movie genres are **exploded** from pipe-separated values (`Crime|Drama` → two entries).
- Age groups: `0-18`, `19-35`, `36-50`, `51+` (from `Lab03Parse.ageGroup()`).
- Year is extracted from Unix timestamp in **UTC** (`Lab03Parse.yearFromTimestamp()`).
- Rating averages are formatted to **4 decimal places** (`Lab03Parse.fmt()`).
- Small lookup tables (movies, users, occupations) are **collected to the driver** as `Map` — suitable for broadcast-style joins given the small dataset size.
- Clean source layout for submission is `spark/java/lab03-rdd/{pom.xml, src/}`; build artifacts in `target/` can be removed and regenerated.

---

## Submission checklist

- [x] Java Spark RDD code committed under `spark/java/lab03-rdd/src/main/java/`.
- [x] Data files organized in `data/` directory.
- [x] Script builds and runs all 6 tasks (`scripts/run_java_rdd_local.sh`).
- [x] All six output files exist and are non-empty in `output/`.
- [x] Screenshot evidence is collected in `screenshots/` (one image per task output).
- [x] README matches actual directory structure and commands.
