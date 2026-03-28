<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 — Lab workspace</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Course-DS200.Q21.1-555?style=for-the-badge" alt="DS200" />
  <img src="https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
</p>

---

## Purpose

This repository is the **course workspace** for **DS200.Q21.1**. Each lab is a **self-contained subdirectory** (data, Hadoop Streaming scripts, optional pandas/Jupyter, outputs) so you can add **Lab02**, **Lab03**, and so on later.

---

## Directory layout

```text
DS200.Q21.1_Lab/
├── README.md                    ← Workspace overview (this file)
├── slides/                      ← Course PDFs (e.g. Hadoop MapReduce tutorial)
│
└── DS200.Q21.1_Lab01/           ← Lab 01 — Movie ratings (MapReduce / Hadoop Streaming)
    ├── README.md                ← Lab 01: structure, run commands, cluster notes
    ├── data/                    ← CSV-style .txt datasets
    ├── hadoop/
    │   ├── streaming/           ← Python mappers & reducers for Streaming
    │   └── run_hadoop_cluster_example.sh
    ├── scripts/
    │   ├── run_hadoop_streaming_local.sh   ← Local MapReduce (sort + pipes)
    │   └── run_all_assignments.py          ← Optional pandas reference
    ├── src/lab01/               ← Optional pandas implementation
    ├── notebooks/               ← assignments.ipynb
    ├── output/                  ← Task reports (.txt)
    └── screenshots/             ← Submission images
```

Add future labs as siblings of `DS200.Q21.1_Lab01/`, for example `DS200.Q21.1_Lab02/`, each with its own `README.md`.

---

## Workflow

1. **IDE:** open **`DS200.Q21.1_Lab`** to see all labs, or open only **`DS200.Q21.1_Lab01`** for Lab 01.
2. **Terminal:** `cd` into the lab folder before running scripts, for example:

   ```bash
   cd /path/to/DS200.Q21.1_Lab/DS200.Q21.1_Lab01
   ```

3. **Docs:** detailed setup, Hadoop local run, HDFS example, and submission steps are in **`DS200.Q21.1_Lab01/README.md`**.

---

## Git and GitHub

- **One repo for the whole course:** `git init` in **`DS200.Q21.1_Lab`** and commit `DS200.Q21.1_Lab01`, …
- **One repo per lab:** `git init` inside each `DS200.Q21.1_Lab0X` if the syllabus requires it.

---

## Example path

`Downloads/DS200.Q21.1_Lab/DS200.Q21.1_Lab01/`

Keep the parent folder **`DS200.Q21.1_Lab`** as the root that contains each **`DS200.Q21.1_Lab0X`** lab directory.
