package ds200.lab04.task5;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.round;

/**
 * Task 5 — Review score statistics: average and count per score level (1–5).
 *
 * Outlier / NULL handling:
 *   - Cast Review_Score to integer (handles non-numeric strings → null).
 *   - Drop rows where Review_Score IS NULL.
 *   - Keep only rows where Review_Score BETWEEN 1 AND 5.
 *
 * Usage: Task5App <dataDir> <outputFile>
 */
public final class Task5App {
  private Task5App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task5App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task5")) {
      Dataset<Row> reviews = readCsv(spark, dataDir + "/Order_Reviews.csv");

      long totalRaw = reviews.count();

      Dataset<Row> clean = reviews
          // try_cast returns null for any non-integer value (dates, empty strings, etc.)
          .withColumn("Score", expr("try_cast(Review_Score as INT)"))
          .filter(col("Score").isNotNull())
          .filter(col("Score").between(1, 5));

      long totalClean = clean.count();

      // Overall average across all valid reviews
      double overallAvg = (double) clean
          .agg(avg("Score").alias("avg"))
          .first().get(0);

      // Per-score-level count
      List<Row> perLevel = clean
          .groupBy("Score")
          .agg(count("Review_ID").alias("ReviewCount"))
          .sort(col("Score").asc())
          .collectAsList();

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 5: Review score statistics (score levels 1–5) ===");
      lines.add("");
      lines.add(String.format("Total rows in dataset          : %,d", totalRaw));
      lines.add(String.format("Valid rows (score 1–5, non-null): %,d", totalClean));
      lines.add(String.format("Overall average review score   : %.4f", overallAvg));
      lines.add("");
      lines.add(String.format("%-8s %s", "Score", "ReviewCount"));
      lines.add("-".repeat(25));
      for (Row row : perLevel) {
        int score = row.getInt(0);
        long cnt = row.getLong(1);
        lines.add(String.format("%-8d %,d", score, cnt));
      }

      for (String line : lines) {
        System.out.println(line);
      }
      OutputWriter.writeLines(outputFile, lines);
      System.out.println("Output written to: " + outputFile);
    }
  }

  private static Dataset<Row> readCsv(SparkSession spark, String path) {
    return spark.read()
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv(path);
  }
}
