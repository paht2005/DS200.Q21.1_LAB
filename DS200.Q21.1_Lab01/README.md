<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 — Big Data Analysis (Lab 01)</b></h1>

### Quick reference

This folder is **`DS200.Q21.1_Lab/DS200.Q21.1_Lab01/`**. The parent workspace overview is in [../README.md](../README.md).

- Run all terminal commands from **`DS200.Q21.1_Lab01`** (the directory that contains `data/`, `hadoop/`, `scripts/`).

---

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.9+" />
  <img src="https://img.shields.io/badge/Hadoop-Streaming-FF6B35?style=for-the-badge" alt="Hadoop Streaming" />
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas" />
  <img src="https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white" alt="Jupyter" />
</p>

---

## Lab overview

Four tasks on a movie–rating dataset (see `notebooks/assignments.ipynb`):

| Task | Idea |
|------|------|
| **1** | Average rating and count per movie (two rating files); report the highest-rated movie with **≥ 5** ratings. |
| **2** | Split multi-genre movies (`|`) and compute average rating per genre. |
| **3** | Join ratings with **users** on `UserID`; male vs female average per movie. |
| **4** | Bucket ages (`0-18`, `18-35`, `35-50`, `50+`); average rating per bucket per movie. |

**Primary implementation:** **Hadoop Streaming**-style MapReduce in Python (`hadoop/streaming/`). Locally, `sort` simulates shuffle/sort; on a cluster, use `hadoop jar … hadoop-streaming-*.jar` (same mapper/reducer scripts—match options to your course slides).

**Reference implementation:** pandas in `src/lab01/` (optional check / notebook).

**Course slides (PDF):** [`../slides/`](../slides/) — e.g. *Slide 2 GFS and Hadoop.pdf*, *Slide 3 Hadoop MapReduce Tutorial.pdf*. This lab follows the usual **map → shuffle/sort → reduce** model and **Hadoop Streaming** packaging (`-mapper`, `-reducer`, `-files`) as in those materials. (PDF text is not quoted here; open the files locally for exact command templates.)

### Assignment notebook vs Hadoop scripts

| `assignments.ipynb` requirement | Hadoop implementation |
|-------------------------------|------------------------|
| **Bài 1:** Both `ratings_1` + `ratings_2`; avg + count per movie; line for best movie with **≥ 5** ratings (cleanup-style) | **Stage 1:** `task1_map_ratings.py` → `task1_reduce_ratings.py` (`MovieID\tsum\tcount`). **Stage 2:** `task1_map_join_movies.py` (side load `movies.txt`) → `task1_reduce_report.py` (single reduce group; final block = cleanup / global max). |
| **Bài 2:** Split `Genres` on `\|`; avg (and count) per genre | `task2_map_genre_rating.py` (replicated genres per rating) → `task2_reduce_avg.py`. |
| **Bài 3:** Join ratings ↔ users on `UserID`; Male_Avg / Female_Avg per movie | `task3_map_gender.py` (users side table) → `task3_reduce_gender.py` (aggregate M/F; reducer loads titles). |
| **Bài 4:** Age buckets 0-18, 18-35, 35-50, 50+; avg per movie per bucket | `task4_map_age.py` → `task4_reduce_age.py` (same bucket rules as `lab01_parse.age_bucket`). |

---

## Student information

| Student ID | Full name        | GitHub                                  | Email                  |
|:----------:|------------------|-----------------------------------------|------------------------|
| 23521143   | Nguyen Cong Phat | [paht2005](https://github.com/paht2005) | 23521143@gm.uit.edu.vn |

---

## Repository layout

```text
DS200.Q21.1_Lab01/
├── README.md                          ← This file
├── requirements.txt                   ← pandas + Jupyter (optional reference path)
├── data/
│   ├── movies.txt
│   ├── ratings_1.txt
│   ├── ratings_2.txt
│   └── users.txt
├── hadoop/
│   ├── streaming/                     ← Python mapper / reducer scripts (English comments)
│   │   ├── lab01_parse.py             ← Shared CSV parsing, age buckets, formatting
│   │   ├── task1_map_ratings.py
│   │   ├── task1_reduce_ratings.py
│   │   ├── task1_map_join_movies.py
│   │   ├── task1_reduce_report.py
│   │   ├── task2_map_genre_rating.py
│   │   ├── task2_reduce_avg.py
│   │   ├── task3_map_gender.py
│   │   ├── task3_reduce_gender.py
│   │   ├── task4_map_age.py
│   │   └── task4_reduce_age.py
│   └── run_hadoop_cluster_example.sh  ← Example HDFS streaming commands (edit JAR / paths)
├── scripts/
│   ├── run_hadoop_streaming_local.sh   ← **Recommended:** local “cluster” via sort + pipes
│   └── run_all_assignments.py         ← Pandas: regenerate output/*.txt (sanity check)
├── src/lab01/                         ← Optional pandas package (loaders + analytics)
├── notebooks/
│   └── assignments.ipynb              ← Assignment wording + runnable pandas cells
├── output/                            ← Generated reports (same filenames from either path)
└── screenshots/                       ← For submission captures
```

**Instructor slides:** parent folder [`../slides/`](../slides/) (PDF). Align cluster `-input`, `-output`, `-files`, and the streaming JAR path with the examples in those slides.

---

## Setup

```bash
cd /path/to/DS200.Q21.1_Lab/DS200.Q21.1_Lab01
python3 -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Run Lab 01 (Hadoop Streaming — local)

This runs the same logic as MapReduce: **map → sort (shuffle) → reduce**. Outputs are written to **`output/`**.

```bash
bash scripts/run_hadoop_streaming_local.sh
```

Requirements: **Python 3** and **`sort`** (default on macOS/Linux). No Java needed for this mode.

Environment overrides (optional):

- `LAB01_MOVIES_FILE` — path to `movies.txt` (used by scripts that load side data).
- `LAB01_USERS_FILE` — path to `users.txt`.

---

## Run on a real Hadoop cluster

1. Copy `data/*.txt` to HDFS (see comments in `hadoop/run_hadoop_cluster_example.sh`).
2. Set **`HADOOP_STREAMING_JAR`** to your `hadoop-streaming-*.jar`.
3. Edit **`HDFS_BASE`** / reducer counts if your slides use different conventions.
4. Run the example script and extend it for tasks 2–4 using the same filenames as in `run_hadoop_streaming_local.sh`.

**Task 1 note:** stage 2 uses **one reducer** so the “highest rated movie (≥ 5 ratings)” line is computed globally (same idea as a reducer `cleanup()` in Java).

---

## Pandas reference (optional)

```bash
python scripts/run_all_assignments.py
```

Options: `--data-dir`, `--out`, or `LAB01_DATA_DIR` (see script help).

---

## Jupyter

```bash
jupyter notebook notebooks/assignments.ipynb
```

Use the project **`.venv`** as the notebook kernel. The notebook uses the pandas package under `src/`.

---

## Submission checklist

1. Source: `hadoop/streaming/`, `scripts/`, `notebooks/`, and (if required) `src/`.
2. Regenerate **`output/`** with `bash scripts/run_hadoop_streaming_local.sh` (or cluster + `hdfs dfs -get`).
3. Screenshots under `screenshots/` (local pipeline, optional cluster UI).
4. Course text file **`<StudentID>.txt`** with your repo URL (`STUDENT_ID.txt.example`).

---

## Notes

- **Encoding:** Hadoop scripts read side files as **UTF-8** (with replacement for bad bytes). Pandas loaders still use **ISO-8859-1** in `io.py` for the original notebook path.
- **Concatenating rating files:** `ratings_1.txt` must end with a **newline** before `cat` with `ratings_2.txt`; the local shell script also inserts a blank line between files to avoid glued lines.
- **Age buckets:** same rule as before (`0-18` includes 18; see `lab01_parse.age_bucket`).
- **MapReduce vs slides:** mapper/reducer boundaries and **`-files` / `-cacheFile`** usage should follow your lab slides; this repo gives working Python logic you can plug into their exact `hadoop jar` template.
