#!/usr/bin/env bash
# Example: run Task 1 on a Hadoop cluster with Hadoop Streaming (adjust paths like on your course slides).
#
# Typical prep:
#   hdfs dfs -mkdir -p /user/$USER/lab01/input
#   hdfs dfs -put /local/path/movies.txt /local/path/ratings_1.txt /local/path/ratings_2.txt /local/path/users.txt /user/$USER/lab01/input/
#
# Find streaming JAR (version varies):
#   ls "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-"*.jar

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STREAM="$ROOT/hadoop/streaming"
DATA="$ROOT/data"

HADOOP_STREAMING_JAR="${HADOOP_STREAMING_JAR:?Set HADOOP_STREAMING_JAR to hadoop-streaming-*.jar}"
HDFS_BASE="${HDFS_BASE:-/user/$USER/lab01}"
HDFS_WORK="$HDFS_BASE/work"

hdfs dfs -rm -r -f "$HDFS_WORK/t1_stage1" "$HDFS_WORK/t1_stage2" || true

# Stage 1: aggregate ratings (two input files; same mapper/reducer as local lab)
hadoop jar "$HADOOP_STREAMING_JAR" \
  -D mapreduce.job.reduces=4 \
  -files "$STREAM/lab01_parse.py,$STREAM/task1_map_ratings.py,$STREAM/task1_reduce_ratings.py" \
  -mapper "python3 task1_map_ratings.py" \
  -reducer "python3 task1_reduce_ratings.py" \
  -input "$HDFS_BASE/input/ratings_1.txt,$HDFS_BASE/input/ratings_2.txt" \
  -output "$HDFS_WORK/t1_stage1"

# Stage 2: join titles + final report (single reducer so the "highest rated" line is global)
hadoop jar "$HADOOP_STREAMING_JAR" \
  -D mapreduce.job.reduces=1 \
  -files "$STREAM/lab01_parse.py,$STREAM/task1_map_join_movies.py,$STREAM/task1_reduce_report.py,$DATA/movies.txt" \
  -mapper "python3 task1_map_join_movies.py movies.txt" \
  -reducer "python3 task1_reduce_report.py" \
  -input "$HDFS_WORK/t1_stage1" \
  -output "$HDFS_WORK/t1_stage2"

echo "hdfs dfs -cat $HDFS_WORK/t1_stage2/part-r-* > task1_movie_ratings.txt"
echo "Tasks 2-4: same pattern with task2_*.py ... task4_*.py and -files including movies.txt / users.txt as in scripts/run_hadoop_streaming_local.sh"
