package ds200.lab04.task6;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

/**
 * Task 6 — Revenue (Price + Freight_Value) in year 2024, grouped by product category.
 *
 * Join chain:
 *   Orders (filter 2024) → Order_Items (on Order_ID) → Products (on Product_ID)
 * Revenue = sum(Price + Freight_Value), sorted descending.
 *
 * Usage: Task6App <dataDir> <outputFile>
 */
public final class Task6App {
  private Task6App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task6App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task6")) {
      Dataset<Row> orders = readCsv(spark, dataDir + "/Orders.csv");
      Dataset<Row> orderItems = readCsv(spark, dataDir + "/Order_Items.csv");
      Dataset<Row> products = readCsv(spark, dataDir + "/Products.csv")
          .select("Product_ID", "Product_Category_Name");

      // Filter orders placed in 2024
      Dataset<Row> orders2024 = orders
          .withColumn("ts",
              to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm"))
          .filter(year(col("ts")).equalTo(2024))
          .select("Order_ID");

      // Join with order items
      Dataset<Row> items2024 = orders2024.join(orderItems, "Order_ID");

      // Join with products to get category
      Dataset<Row> withCategory = items2024.join(products, "Product_ID");

      // Compute revenue
      List<Row> result = withCategory
          .withColumn("Revenue", col("Price").plus(col("Freight_Value")))
          .groupBy("Product_Category_Name")
          .agg(round(sum("Revenue"), 2).alias("TotalRevenue"))
          .sort(desc("TotalRevenue"))
          .collectAsList();

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 6: Revenue in 2024 by product category (descending) ===");
      lines.add("");
      lines.add(String.format("%-45s %s", "Category", "TotalRevenue (EUR)"));
      lines.add("-".repeat(65));
      for (Row row : result) {
        String cat = row.isNullAt(0) ? "(uncategorized)" : row.getString(0);
        double rev = row.isNullAt(1) ? 0.0 : ((Number) row.get(1)).doubleValue();
        lines.add(String.format("%-45s %,.2f", cat, rev));
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
