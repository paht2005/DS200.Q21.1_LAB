package ds200.lab04.task4;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

/**
 * Task 4 — Number of orders grouped by year and month.
 *
 * Sort: year ascending, month descending.
 * Order_Purchase_Timestamp format: "yyyy-MM-dd HH:mm"
 *
 * Usage: Task4App <dataDir> <outputFile>
 */
public final class Task4App {
  private Task4App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task4App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task4")) {
      Dataset<Row> orders = readCsv(spark, dataDir + "/Orders.csv");

      Dataset<Row> withTime = orders
          .withColumn("ts",
              to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm"))
          .withColumn("Year", year(col("ts")))
          .withColumn("Month", month(col("ts")));

      List<Row> result = withTime
          .groupBy("Year", "Month")
          .agg(count("Order_ID").alias("OrderCount"))
          .sort(asc("Year"), desc("Month"))
          .collectAsList();

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 4: Orders by year and month (year asc, month desc) ===");
      lines.add("");
      lines.add(String.format("%-6s %-6s %s", "Year", "Month", "OrderCount"));
      lines.add("-".repeat(30));
      for (Row row : result) {
        int yr = row.isNullAt(0) ? -1 : row.getInt(0);
        int mo = row.isNullAt(1) ? -1 : row.getInt(1);
        long cnt = row.getLong(2);
        lines.add(String.format("%-6d %-6d %,d", yr, mo, cnt));
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
