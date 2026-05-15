package ds200.lab04.task1;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Task 1 — Read all CSV files using inferSchema and report schemas + row counts.
 *
 * Usage: Task1App <dataDir> <outputFile>
 */
public final class Task1App {
  private Task1App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task1App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task1")) {
      Dataset<Row> orders = readCsv(spark, dataDir + "/Orders.csv");
      Dataset<Row> customers = readCsv(spark, dataDir + "/Customer_List.csv");
      Dataset<Row> orderItems = readCsv(spark, dataDir + "/Order_Items.csv");
      Dataset<Row> products = readCsv(spark, dataDir + "/Products.csv");
      Dataset<Row> reviews = readCsv(spark, dataDir + "/Order_Reviews.csv");

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 1: Load CSV files with inferSchema ===");
      lines.add("");

      appendDatasetInfo(lines, "Orders", orders);
      appendDatasetInfo(lines, "Customer_List", customers);
      appendDatasetInfo(lines, "Order_Items", orderItems);
      appendDatasetInfo(lines, "Products", products);
      appendDatasetInfo(lines, "Order_Reviews", reviews);

      lines.add("All 5 datasets loaded successfully.");

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

  private static void appendDatasetInfo(List<String> lines, String name, Dataset<Row> df) {
    lines.add("--- " + name + " ---");
    lines.add("Row count : " + df.count());
    lines.add("Column count: " + df.columns().length);
    lines.add("Schema:");
    for (org.apache.spark.sql.types.StructField f : df.schema().fields()) {
      lines.add("  " + f.name() + " : " + f.dataType().simpleString());
    }
    lines.add("");
  }
}
