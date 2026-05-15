package ds200.lab04.task3;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;

/**
 * Task 3 — Number of orders per country, sorted descending.
 *
 * Join Orders with Customer_List on Customer_Trx_ID to get the country,
 * then group by Customer_Country and count, sorted by count desc.
 *
 * Usage: Task3App <dataDir> <outputFile>
 */
public final class Task3App {
  private Task3App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task3App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task3")) {
      Dataset<Row> orders = readCsv(spark, dataDir + "/Orders.csv");
      Dataset<Row> customers = readCsv(spark, dataDir + "/Customer_List.csv")
          .select("Customer_Trx_ID", "Customer_Country");

      Dataset<Row> joined = orders.join(customers, "Customer_Trx_ID");

      List<Row> result = joined
          .groupBy("Customer_Country")
          .agg(count("Order_ID").alias("OrderCount"))
          .sort(desc("OrderCount"))
          .collectAsList();

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 3: Number of orders by country (descending) ===");
      lines.add("");
      lines.add(String.format("%-40s %s", "Country", "OrderCount"));
      lines.add("-".repeat(55));
      for (Row row : result) {
        String country = row.isNullAt(0) ? "(unknown)" : row.getString(0);
        long cnt = row.getLong(1);
        lines.add(String.format("%-40s %,d", country, cnt));
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
