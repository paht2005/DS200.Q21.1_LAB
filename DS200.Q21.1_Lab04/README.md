<p align="center">
  <a href="https://www.uit.edu.vn/" title="University of Information Technology">
    <img src="https://i.imgur.com/WmMnSRt.png" alt="University of Information Technology (UIT)" width="400">
  </a>
</p>

<h1 align="center"><b>DS200.Q21.1 - Big Data Analysis (Lab 04)</b></h1>

<p align="center">
  <img src="https://img.shields.io/badge/Java-11+-orange?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java 11+" />
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark 3.5.x" />
  <img src="https://img.shields.io/badge/DataFrame-Java-blue?style=for-the-badge" alt="Java DataFrame" />
</p>

<p align="center">
  <a href="https://github.com/paht2005"><img src="https://img.shields.io/badge/GitHub-paht2005-181717?style=flat-square&logo=github" alt="GitHub" /></a>
  <a href="https://www.linkedin.com/in/ncp2005/"><img src="https://img.shields.io/badge/LinkedIn-Phat%20Nguyen-0A66C2?style=flat-square&logo=linkedin" alt="LinkedIn" /></a>
  <a href="mailto:23521143@gm.uit.edu.vn"><img src="https://img.shields.io/badge/Email-23521143%40gm.uit.edu.vn-EA4335?style=flat-square&logo=gmail&logoColor=white" alt="Email" /></a>
</p>

**Lab folder:** `DS200.Q21.1_Lab/DS200.Q21.1_Lab04/` — Parent overview: [../README.md](../README.md)

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
5. [End-to-end data flow](#end-to-end-data-flow)
6. [Task-by-task details](#task-by-task-details)
7. [Prerequisites](#prerequisites)
8. [Run all tasks](#run-all-tasks)
9. [Run each task individually](#run-each-task-individually)
10. [Output files](#output-files)
11. [Screenshots](#screenshots)
12. [Submission checklist](#submission-checklist)

---

## Objective

Implement the Fecom e-commerce analytics assignments from `assignments.ipynb` using **Java Spark DataFrame** (SparkSession + Dataset\<Row\>).  
Each task reads semicolon-delimited CSV files, processes them through DataFrame operations, and writes a plain-text report.

Tasks implemented: **1, 2, 3, 4, 5** (required) + **6, 7, 10** (3 optional chosen from 6–10).

---

## Repository layout

```text
DS200.Q21.1_Lab04/
├── README.md                        ← This file
├── assignments.ipynb                ← Assignment descriptions (Vietnamese)
├── data/
│   ├── Orders.csv                   ← Order_ID; Customer_Trx_ID; status; timestamps
│   ├── Customer_List.csv            ← Customer demographics + country
│   ├── Order_Items.csv              ← Product, Seller, Price, Freight per line item
│   ├── Products.csv                 ← Product_ID, category, dimensions
│   └── Order_Reviews.csv            ← Review_ID, Order_ID, Review_Score, comments
├── scripts/
│   ├── run_java_dataframe_local.sh  ← Main script: build JAR + run all tasks
│   ├── java.sh                      ← Convenience wrapper
│   └── screenshots.sh               ← Display task outputs with student header for screenshotting
├── spark/
│   └── java/
│       └── lab04-dataframe/
│           ├── pom.xml              ← Maven config (Spark 3.5.x, Java 11)
│           └── src/main/java/ds200/lab04/
│               ├── util/            ← SparkSessions, OutputWriter
│               ├── task1/           ← Task1App  (load datasets + schema info)
│               ├── task2/           ← Task2App  (overall stats)
│               ├── task3/           ← Task3App  (orders by country)
│               ├── task4/           ← Task4App  (orders by year / month)
│               ├── task5/           ← Task5App  (review score stats)
│               ├── task6/           ← Task6App  (revenue 2024 by category)
│               ├── task7/           ← Task7App  (top products + avg rating)
│               └── task10/          ← Task10App (seller ranking)
├── output/                          ← Generated reports (8 .txt files)
└── screenshots/                     ← Submission screenshots
```

---

## Dataset

All CSV files use **semicolon (`;`)** as the delimiter.

| File | Key columns | Description |
|------|-------------|-------------|
| `Orders.csv` | `Order_ID`, `Customer_Trx_ID`, `Order_Status`, `Order_Purchase_Timestamp` | 99,441 orders (2022–2024) |
| `Customer_List.csv` | `Customer_Trx_ID`, `Customer_Country`, `Age`, `Gender` | 102,727 unique customers |
| `Order_Items.csv` | `Order_ID`, `Product_ID`, `Seller_ID`, `Price`, `Freight_Value` | Line items per order |
| `Products.csv` | `Product_ID`, `Product_Category_Name` | 32,951 products, 72 categories |
| `Order_Reviews.csv` | `Order_ID`, `Review_Score` | Scores 1–5, may contain nulls |

---

## Assignments implemented

| # | Task | Java class | Output file |
|---|------|------------|-------------|
| 1 | Load all CSV files with `inferSchema`, report schemas + row counts | `Task1App` | `task1_load_datasets.txt` |
| 2 | Total orders, unique customers, unique sellers | `Task2App` | `task2_overall_stats.txt` |
| 3 | Orders per country — sorted descending | `Task3App` | `task3_orders_by_country.txt` |
| 4 | Orders by year / month (year asc, month desc) | `Task4App` | `task4_orders_by_year_month.txt` |
| 5 | Avg + count per review score level (1–5); handle nulls & outliers | `Task5App` | `task5_review_stats.txt` |
| 6 | Revenue (Price + Freight) in 2024 by product category | `Task6App` | `task6_revenue_2024_by_category.txt` |
| 7 | Top-selling products + avg review score per product | `Task7App` | `task7_top_products.txt` |
| 10 | Seller ranking by total revenue + order count (DENSE_RANK) | `Task10App` | `task10_seller_ranking.txt` |

---

## End-to-end data flow

```
data/*.csv  ──inferSchema──▶  Dataset<Row>
                                  │
                ┌─────────────────┴──────────────────────┐
                │  Task1: schema report                   │
                │  Task2: count / countDistinct           │
                │  Task3: join Orders × Customers         │
                │  Task4: year() / month() + groupBy      │
                │  Task5: cast + filter + groupBy         │
                │  Task6: filter year=2024, join × 2      │
                │  Task7: groupBy + join reviews          │
                │  Task10: sum + DENSE_RANK window        │
                └───────────────────┬────────────────────┘
                                    │
                              output/*.txt
```

---

## Task-by-task details

### Task 1 — Load datasets with inferSchema
- Reads each CSV with `option("sep", ";")` and `option("inferSchema", "true")`.
- Reports column names, inferred types, and row counts.

### Task 2 — Overall statistics
- `orders.count()` → total orders.
- `countDistinct("Customer_Trx_ID")` on Customer_List → unique customers.
- `countDistinct("Seller_ID")` on Order_Items → unique sellers.

### Task 3 — Orders by country
- Inner join `Orders` ⟕ `Customer_List` on `Customer_Trx_ID`.
- `groupBy("Customer_Country").agg(count("Order_ID"))` sorted `desc`.

### Task 4 — Orders by year / month
- Parse `Order_Purchase_Timestamp` with `to_timestamp(..., "yyyy-MM-dd HH:mm")`.
- Extract `year()` and `month()`, group, sort `year asc, month desc`.

### Task 5 — Review score statistics
- Cast `Review_Score` to `IntegerType` (non-numeric → null).
- Filter: `isNotNull()` AND `between(1, 5)` to drop outliers.
- `groupBy("Score").agg(count(...))` + overall `avg`.

### Task 6 — Revenue in 2024 by category (optional)
- Filter `Orders` where `year(ts) = 2024`.
- Join with `Order_Items` and then `Products`.
- `sum(Price + Freight_Value)` grouped by `Product_Category_Name`, sorted `desc`.

### Task 7 — Top products + avg rating (optional)
- Sales count: `groupBy("Product_ID").agg(count("Order_Item_ID"))`.
- Avg rating: join `Order_Items` with clean `Order_Reviews` on `Order_ID`, then `avg(Score)` per `Product_ID`.
- Left-join sales count with avg rating, sort by `SalesCount desc`.

### Task 10 — Seller ranking (optional)
- `sum(Price + Freight_Value)` + `countDistinct("Order_ID")` per `Seller_ID`.
- Window function `DENSE_RANK() OVER (ORDER BY TotalRevenue DESC)`.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Java (JDK) | 11 or higher |
| Apache Maven | 3.6+ |
| Apache Spark | 3.5.x (with `spark-submit` on PATH) |

---

## Run all tasks

```bash
cd DS200.Q21.1_Lab04
chmod +x scripts/run_java_dataframe_local.sh
scripts/run_java_dataframe_local.sh
```

The script:
1. Builds the JAR via `mvn package`.
2. Submits each task with `spark-submit --master local[*]`.
3. Writes all reports to `output/`.

---

## Screenshots

### How to capture

Run `screenshots.sh` to display each task's output with a student info header:

```bash
# Display all tasks one by one (press Enter between each)
scripts/screenshots.sh

# Display a specific task only (e.g. step 3)
scripts/screenshots.sh 3
```

### Naming convention

Files in `screenshots/` follow the pattern `ssNN_<description>.png`:

| File(s) | Content |
|---------|---------|
| `ss01_identity.png` | Environment info: whoami, Java, Maven, Spark versions |
| `ss02_task1_load_datasets.png` | Task 1 — CSV schemas + row counts |
| `ss03_task2_overall_stats.png` | Task 2 — Total orders / customers / sellers |
| `ss04_task3_orders_by_country.png` | Task 3 — Orders by country (descending) |
| `ss05_task4_orders_by_year_month.png` | Task 4 — Orders grouped by year and month |
| `ss06_task5_review_stats.png` | Task 5 — Review score statistics |
| `ss07_task6_revenue_2024.png` | Task 6 — Revenue 2024 by product category |
| `ss08_task7_top_products.png` | Task 7 — Top-selling products + avg review score *(see note)* |
| `ss09_task10_seller_ranking.png` | Task 10 — Seller ranking by revenue + order count *(see note)* |

**Multi-part screenshots:** When an output is too long to fit in one screen, it is split into numbered parts — e.g. `ss02_task1_load_datasets_01.png`, `ss02_task1_load_datasets_01.png`.

**Truncated screenshots (tasks 7 and 10):** These outputs contain hundreds of rows (all products / all 3,095 sellers). The screenshots capture the bash script invocation and the top portion of the results only.  
→ **Full results are in `output/task7_top_products.txt` and `output/task10_seller_ranking.txt`.**

---

## Run each task individually

After building the JAR once:

```bash
JAR=spark/java/lab04-dataframe/target/lab04-dataframe-1.0.0.jar
DATA=data
OUT=output

# Task 1
spark-submit --master local[*] --class ds200.lab04.task1.Task1App $JAR $DATA $OUT/task1_load_datasets.txt

# Task 2
spark-submit --master local[*] --class ds200.lab04.task2.Task2App $JAR $DATA $OUT/task2_overall_stats.txt

# Task 3
spark-submit --master local[*] --class ds200.lab04.task3.Task3App $JAR $DATA $OUT/task3_orders_by_country.txt

# Task 4
spark-submit --master local[*] --class ds200.lab04.task4.Task4App $JAR $DATA $OUT/task4_orders_by_year_month.txt

# Task 5
spark-submit --master local[*] --class ds200.lab04.task5.Task5App $JAR $DATA $OUT/task5_review_stats.txt

# Task 6
spark-submit --master local[*] --class ds200.lab04.task6.Task6App $JAR $DATA $OUT/task6_revenue_2024_by_category.txt

# Task 7
spark-submit --master local[*] --class ds200.lab04.task7.Task7App $JAR $DATA $OUT/task7_top_products.txt

# Task 10
spark-submit --master local[*] --class ds200.lab04.task10.Task10App $JAR $DATA $OUT/task10_seller_ranking.txt
```

---

## Output files

| File | Content |
|------|---------|
| `output/task1_load_datasets.txt` | Schema + row count per dataset |
| `output/task2_overall_stats.txt` | Total orders / customers / sellers |
| `output/task3_orders_by_country.txt` | Country → OrderCount (desc) |
| `output/task4_orders_by_year_month.txt` | Year, Month, OrderCount |
| `output/task5_review_stats.txt` | Score → ReviewCount + overall avg |
| `output/task6_revenue_2024_by_category.txt` | Category → TotalRevenue (2024) |
| `output/task7_top_products.txt` | ProductID → SalesCount + AvgReviewScore |
| `output/task10_seller_ranking.txt` | Rank, SellerID, Revenue, OrderCount |

---

## Submission checklist

- [x] `spark/java/lab04-dataframe/pom.xml` — Maven project (Spark 3.5.x, Java 11)
- [x] `spark/java/lab04-dataframe/src/…` — 8 task apps + 2 utilities
- [x] `scripts/run_java_dataframe_local.sh` — one-shot build + run script
- [x] `output/*.txt` — 8 result files
- [x] `screenshots/` — terminal screenshots (full results in `output/*.txt`)
- [x] `README.md` — this file
