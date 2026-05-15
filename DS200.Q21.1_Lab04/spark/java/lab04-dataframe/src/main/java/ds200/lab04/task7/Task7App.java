package ds200.lab04.task7;

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
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.round;

/**
 * Task 7 — Top-selling products and average review score per product.
 *
 * Sales count: count of Order_Item_ID per Product_ID in Order_Items.
 * Avg review : join Order_Items → Order_Reviews (on Order_ID), then average
 *              clean Review_Score (1–5, non-null) per Product_ID.
 * Result sorted by SalesCount descending.
 *
 * Usage: Task7App <dataDir> <outputFile>
 */
public final class Task7App {
  private Task7App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task7App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task7")) {
      Dataset<Row> orderItems = readCsv(spark, dataDir + "/Order_Items.csv");
      Dataset<Row> reviews = readCsv(spark, dataDir + "/Order_Reviews.csv");

      // Sales count per product
      Dataset<Row> salesCount = orderItems
          .groupBy("Product_ID")
          .agg(count("Order_Item_ID").alias("SalesCount"));

      // Clean reviews and compute avg score per order
      Dataset<Row> cleanReviews = reviews
          .withColumn("Score", expr("try_cast(Review_Score as INT)"))
          .filter(col("Score").isNotNull())
          .filter(col("Score").between(1, 5))
          .select("Order_ID", "Score");

      // Join Order_Items with clean reviews to map review → product
      Dataset<Row> itemReviews = orderItems
          .select("Order_ID", "Product_ID")
          .join(cleanReviews, "Order_ID");

      // Average score per product
      Dataset<Row> avgScore = itemReviews
          .groupBy("Product_ID")
          .agg(round(avg("Score"), 4).alias("AvgReviewScore"));

      // Combine sales count + avg score
      Dataset<Row> result = salesCount
          .join(avgScore, "Product_ID", "left")
          .sort(desc("SalesCount"));

      List<Row> rows = result.collectAsList();

      // Identify top-selling product
      String topProduct = rows.isEmpty() ? "N/A" : rows.get(0).getString(0);
      long topSales = rows.isEmpty() ? 0L : rows.get(0).getLong(1);

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 7: Top-selling products and average review score ===");
      lines.add("");
      lines.add("Top-selling product : " + topProduct);
      lines.add("Sales count         : " + topSales);
      lines.add("");
      lines.add(String.format("%-36s %-12s %s", "Product_ID", "SalesCount", "AvgReviewScore"));
      lines.add("-".repeat(62));
      for (Row row : rows) {
        String pid = row.isNullAt(0) ? "(unknown)" : row.getString(0);
        long cnt = row.getLong(1);
        String score = row.isNullAt(2) ? "N/A" : String.format("%.4f", ((Number) row.get(2)).doubleValue());
        lines.add(String.format("%-36s %-12d %s", pid, cnt, score));
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
