package ds200.lab04.task2;

import ds200.lab04.util.OutputWriter;
import ds200.lab04.util.SparkSessions;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.countDistinct;

/**
 * Task 2 — Count total orders, distinct customers, and distinct sellers.
 *
 * - Total orders      : count of rows in Orders (each row is one order)
 * - Total customers   : count distinct Customer_Trx_ID in Customer_List
 * - Total sellers     : count distinct Seller_ID in Order_Items
 *
 * Usage: Task2App <dataDir> <outputFile>
 */
public final class Task2App {
  private Task2App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: Task2App <dataDir> <outputFile>");
    }
    String dataDir = args[0];
    String outputFile = args[1];

    try (SparkSession spark = SparkSessions.local("DS200-Lab04-Task2")) {
      Dataset<Row> orders = readCsv(spark, dataDir + "/Orders.csv");
      Dataset<Row> customers = readCsv(spark, dataDir + "/Customer_List.csv");
      Dataset<Row> orderItems = readCsv(spark, dataDir + "/Order_Items.csv");

      long totalOrders = orders.count();
      long totalCustomers = (long) customers
          .agg(countDistinct("Customer_Trx_ID"))
          .first().get(0);
      long totalSellers = (long) orderItems
          .agg(countDistinct("Seller_ID"))
          .first().get(0);

      List<String> lines = new ArrayList<>();
      lines.add("=== Task 2: Total orders, customers, and sellers ===");
      lines.add("");
      lines.add(String.format("%-25s : %,d", "Total Orders", totalOrders));
      lines.add(String.format("%-25s : %,d", "Unique Customers", totalCustomers));
      lines.add(String.format("%-25s : %,d", "Unique Sellers", totalSellers));

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
