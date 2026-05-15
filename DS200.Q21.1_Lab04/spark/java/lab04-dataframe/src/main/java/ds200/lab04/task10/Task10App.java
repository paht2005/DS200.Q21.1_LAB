package ds200.lab04.task10;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.dense_rank;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

/**
 * Task 10 — Rank sellers by total revenue and number of orders.
 *
 * Revenue = sum(Price + Freight_Value) per Seller_ID in Order_Items.
 * OrderCount = count(distinct Order_ID) per Seller_ID.
 * Ranking uses DENSE_RANK() over (ORDER BY TotalRevenue DESC).
 *
 * Usage: Task10App <dataDir> <outputFile>
 */
public final class Task10App {
  private Task10App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task10App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task10")) {
      Dataset<Row> orderItems = readCsv(spark, dataDir + "/Order_Items.csv");

      Dataset<Row> sellerStats = orderItems
          .withColumn("Revenue", col("Price").plus(col("Freight_Value")))
          .groupBy("Seller_ID")
          .agg(
              round(sum("Revenue"), 2).alias("TotalRevenue"),
              countDistinct("Order_ID").alias("OrderCount")
          );

      WindowSpec windowSpec = Window.orderBy(desc("TotalRevenue"), desc("OrderCount"));

      Dataset<Row> ranked = sellerStats
          .withColumn("Rank", dense_rank().over(windowSpec))
          .sort(col("Rank").asc());

      List<Row> rows = ranked.collectAsList();

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 10: Seller ranking by total revenue ===");
      lines.add("");
      lines.add(String.format("%-6s %-36s %-20s %s", "Rank", "Seller_ID", "TotalRevenue (EUR)", "OrderCount"));
      lines.add("-".repeat(75));
      for (Row row : rows) {
        int rank = row.getInt(3);
        String seller = row.isNullAt(0) ? "(unknown)" : row.getString(0);
        double rev = row.isNullAt(1) ? 0.0 : ((Number) row.get(1)).doubleValue();
        long cnt = row.getLong(2);
        lines.add(String.format("%-6d %-36s %-20.2f %,d", rank, seller, rev, cnt));
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
