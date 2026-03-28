<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 01)</b></h1>

**Lab folder:** `DS200.Q21.1_Lab/DS200.Q21.1_Lab01/` - Parent overview: [../README.md](../README.md)

Run terminal commands from **`DS200.Q21.1_Lab01`** (the  directory that contains `data/`, `hadoop/`, `scripts/`).

---


<p align="center">
  <img src="https://img.shields.io/badge/Java-11+-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 11+" />
  <img src="https://img.shields.io/badge/Hadoop-3.x-FF6B35?style=for-the-badge" alt="Hadoop 3.x" />
  <img src="https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.9+ (optional)" />
  <img src="https://img.shields.io/badge/Pandas-optional-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas optional" />
  <img src="https://img.shields.io/badge/Jupyter-optional-F37626?style=for-the-badge&logo=jupyter&logoColor=white" alt="Jupyter optional" />
</p>


---

## Outline

1. [Student information](#student-information)
2. [Repository layout](#repository-layout)
3. [What this lab does](#what-this-lab-does)
4. [Which code is "main": Java vs Python](#which-code-is-main-java-vs-python)
5. [End-to-end data flow (MapReduce mental model)](#end-to-end-data-flow-mapreduce-mental-model)
6. [Task-by-task flow (Java implementation)](#task-by-task-flow-java-implementation)
7. [Prerequisites](#prerequisites)
8. [Run: Java MapReduce](#run-java-mapreduce)
9. [Run: Hadoop Streaming (Python)](#run-hadoop-streaming-python)
10. [Run: Pandas and Jupyter](#run-pandas-and-jupyter)
11. [Run on a real Hadoop cluster](#run-on-a-real-hadoop-cluster)
12. [Submission checklist](#submission-checklist)
13. [Notes (encoding, age buckets, concat ratings)](#notes-encoding-age-buckets-concat-ratings)


---

## Student information

| Student ID | Full name        | GitHub                                  | Email                  |
|:----------:|------------------|-----------------------------------------|------------------------|
| 23521143   | Nguyen Cong Phat | [paht2005](https://github.com/paht2005) | 23521143@gm.uit.edu.vn |


---

## Repository layout

```text
DS200.Q21.1_Lab01/
├── README.md                          <- This file
├── requirements.txt                   <- pandas + Jupyter (optional)
├── data/                              <- movies, ratings_1/2, users
├── hadoop/
│   ├── java/
│   │   ├── README.md                  <- Short pointer + build reminder
│   │   └── lab01-mapreduce/           <- Maven Java MapReduce (sources committed; target/ gitignored)
│   ├── streaming/                     <- Optional Python Streaming mappers/reducers
│   └── run_hadoop_cluster_example.sh
├── scripts/
│   ├── run_java_mapreduce_local.sh    <- Build JAR + run all 4 Java drivers (needs hadoop + mvn)
│   ├── run_hadoop_streaming_local.sh  <- Optional: Python + sort pipeline
│   └── run_all_assignments.py         <- Optional: pandas -> output/
├── src/lab01/                         <- Optional pandas package
├── notebooks/assignments.ipynb
├── output/                            <- Generated reports
└── screenshots/                       <- images for reporting
```

**Course slides (PDF):** [`../slides/`](../slides/) - align HDFS paths, `-files`, and streaming JAR commands with the instructor's templates.

---

## What this lab does

Four tasks on a movie-rating dataset (full wording: `notebooks/assignments.ipynb`):

| Task | Goal |
|------|------|
| **1** | Average rating and **total count** per movie using **both** `ratings_1.txt` and `ratings_2.txt`; one extra line for the **highest average** among movies with **at least 5** ratings (reducer `cleanup()` style). |
| **2** | Each movie can have **multiple genres** (`|` separated). **Explode** genres so each rating contributes to **every** genre of that movie; then **average** (and count) **per genre**. |
| **3** | **Join** ratings with **users** on `UserID`; for each movie, **male vs female** average rating (by title). |
| **4** | Bucket user ages (`0-18`, `18-35`, `35-50`, `50+`); for each movie, **average rating per bucket**. |

Generated reports (same filenames regardless of Java or Python path):

- `output/task1_movie_ratings.txt`
- `output/task2_genre_ratings.txt`
- `output/task3_gender_by_movie.txt`
- `output/task4_age_groups_by_movie.txt`

---

## Which code is "main": Java vs Python

| Path | Language | Role |
|------|----------|------|
| **`hadoop/java/lab01-mapreduce/`** | **Java** | **Primary MapReduce solution** for the course: Hadoop MapReduce API, `Job`, mappers and reducers, **`Job.addCacheFile`** for small side tables (`movies.txt`, `users.txt`). **All source comments are in English.** |
| **`hadoop/streaming/*.py`** | **Python** | **Optional alternative** with the **same MapReduce logic** using **Hadoop Streaming** (stdin/stdout, tab-separated keys/values). Matches slides that use `hadoop jar ... hadoop-streaming-*.jar` with `-mapper`, `-reducer`, `-files`. |
| **`scripts/run_hadoop_streaming_local.sh`** | Bash + Python | **Local pipeline**: Unix **`sort`** replaces shuffle/sort; **no Java** required. Useful for quick laptop checks. |
| **`src/lab01/` + `scripts/run_all_assignments.py`** | **Pandas** | **Optional reference**: same metrics in memory for **sanity checks** or plots; **not** distributed. |
| **`notebooks/assignments.ipynb`** | Python / Markdown | **Assignment text** plus optional runnable pandas cells tied to `src/lab01/`. |

**Summary:** Commit **all Java source**; `.gitignore` only skips **Maven `target/`** (build output), **not** `*.java`. Python **sources** are tracked too; `.gitignore` skips **`.venv/`, `__pycache__`,** and similar local artifacts only.

---

## End-to-end data flow (MapReduce mental model)

Every task follows the same pattern:

```text
Input split (for example merged ratings lines)
    -> MAP: parse line, emit (intermediate_key, intermediate_value)
    -> SHUFFLE and SORT: Hadoop groups all values with the same key (the Streaming shell script uses `sort` instead)
    -> REDUCE: aggregate values for that key, emit final (or next-stage) records
```

**Side tables** (small enough for this lab) use a **replicated join**:

- **Java:** `job.addCacheFile(...)` so each task can read `movies.txt` / `users.txt` locally.
- **Python Streaming:** same idea via `-files` or environment variables pointing at `movies.txt` or `users.txt` (see streaming scripts).

---

## Task-by-task flow (Java implementation)

### Task 1 - two chained jobs

1. **Stage 1 - aggregate ratings**  
   - **Mapper:** each rating line -> `(MovieID, Rating)`.  
   - **Reducer:** per `MovieID`, sum ratings and count rows -> `MovieID \t sum \t count`.  
   - **Why two stages?** Stage 1 has no titles; titles live in `movies.txt`.

2. **Stage 2 - join titles and human-readable report**  
   - **Distributed cache:** `movies.txt` (`MovieID` -> `Title`).  
   - **Mapper:** read stage-1 lines, attach title, compute average -> emit **one constant key** (`TASK1_REPORT`) so **one reducer** receives **all** movies.  
   - **Reducer:** sort by title, print `Title AverageRating: ... (TotalRatings: ...)`, blank line, then in **`cleanup()`** print the **best** movie among those with **at least 5** ratings (or a fixed message if none qualify).

**Classes:** `ds200.lab01.task1.*` (`Task1Driver`, `Task1RatingsMapper`, `Task1RatingsReducer`, `Task1ReportMapper`, `Task1ReportReducer`).

### Task 2 - explode genres

- **Cache:** `movies.txt` -> per `MovieID`, list of genres (`|` split).  
- **Mapper:** per rating, for **each** genre of that movie -> `(Genre, Rating)`.  
- **Reducer:** per genre, sum and count -> one line: `Genre: avg (TotalRatings: n)`.

**Classes:** `ds200.lab01.task2.*`.

### Task 3 - gender split per movie

- **Cache:** `users.txt` in the **mapper** (join on `UserID`); `movies.txt` in the **reducer** (resolve title).  
- **Mapper:** `(MovieID, Gender\tRating)` for `M` / `F` only.  
- **Reducer:** separate sums and counts for M and F -> `Title: maleAvg, femaleAvg` (`N/A` if a side has no ratings).

**Classes:** `ds200.lab01.task3.*`.

### Task 4 - age buckets per movie

- **Cache:** `users.txt` (age) in mapper; `movies.txt` in reducer.  
- **Mapper:** bucket age with `Lab01Parse.ageBucket` -> `(MovieID, Bucket\tRating)`.  
- **Reducer:** four fixed buckets in order -> `Title: [0-18: ..., 18-35: ..., ...]`.

**Classes:** `ds200.lab01.task4.*`.

**Shared helpers:** `ds200.lab01.Lab01Parse` (CSV parsing, `fmtRating`, age buckets), `ds200.lab01.SideTables` (load cached files using **ISO-8859-1** for side files).



---

## Prerequisites

**For Java (primary):**

- JDK **11+**
- **Apache Hadoop 3.x** on `PATH` (`hadoop` command; version should match `hadoop.version` in `hadoop/java/lab01-mapreduce/pom.xml`, default **3.3.6**)
- **Maven** (`mvn`)

**For optional Python paths:**

- Python **3.9+**, `pip install -r requirements.txt`, `sort` (macOS/Linux)

---

## Run: Java MapReduce

**Default for this course:** use this path when you submit Java + Hadoop work.

```bash
cd /path/to/DS200.Q21.1_Lab/DS200.Q21.1_Lab01
bash scripts/run_java_mapreduce_local.sh
```

This script runs `mvn package`, merges the two ratings files (with a blank line between them, same as the Python script), runs each **`Task*Driver`** with **local** MapReduce (`mapreduce.framework.name=local`, `fs.defaultFS=file:///`), then **sorts** task 3 and 4 outputs like the streaming script.

**Manual build:**

```bash
cd hadoop/java/lab01-mapreduce
mvn -q package -DskipTests
# JAR: target/lab01-mapreduce-1.0.0.jar
```

Main classes: `ds200.lab01.task1.Task1Driver` through `task4.Task4Driver` (see `hadoop/java/README.md` for argument lists).

---

## Run: Hadoop Streaming (Python)

Same MapReduce logic, but mappers and reducers are **Python processes** over stdin/stdout:

```bash
bash scripts/run_hadoop_streaming_local.sh
```

Requires **Python 3** and `sort` only. Optional env: `LAB01_MOVIES_FILE`, `LAB01_USERS_FILE`.

---

## Run: Pandas and Jupyter

```bash
python scripts/run_all_assignments.py
jupyter notebook notebooks/assignments.ipynb
```

Use the project `.venv` as the notebook kernel if you use a virtualenv.

---

## Run on a real Hadoop cluster

1. Copy `data/*.txt` to HDFS (see `hadoop/run_hadoop_cluster_example.sh` for Streaming).  
2. For **Java:** upload the JAR, use HDFS input/output paths, keep **`addCacheFile`** (or your cluster equivalent) for `movies.txt` / `users.txt`, and **remove or override** the local-only settings inside the drivers if your cluster requires YARN.  
3. For **Streaming:** set `HADOOP_STREAMING_JAR`, edit `HDFS_BASE`, wire the same script names as in `run_hadoop_streaming_local.sh`.

**Task 1:** stage 2 must keep **one reducer** (or equivalent) so the "highest rated movie (at least 5 ratings)" line is **global**.

---

## Submission checklist

1. Source: **`hadoop/java/lab01-mapreduce/src/`** (Java), plus **`hadoop/streaming/`**, **`scripts/`**, **`notebooks/`**, and (if required) **`src/`**.  
2. Regenerate **`output/`** with **`bash scripts/run_java_mapreduce_local.sh`** (or cluster + `hdfs dfs -get`).  
3. Screenshots under `screenshots/` if required.  
4. Course text file **`<StudentID>.txt`** with your repo URL (`STUDENT_ID.txt.example`).

---

## Notes (encoding, age buckets, concat ratings)

- **Side files in Java:** read with **ISO-8859-1** (`Lab01Parse.sideFileCharset()`) to match common lab exports.  
- **Python streaming** side readers use **UTF-8** with replacement; pandas loaders may use **ISO-8859-1** in `io.py`.  
- **Concatenating ratings:** keep a **newline** between `ratings_1.txt` and `ratings_2.txt` so lines do not merge; both run scripts handle this.  
- **Age buckets:** `0-18` includes age **18** (see `Lab01Parse.ageBucket`).
