package ds200.lab03.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public final class SparkContexts {
  private SparkContexts() {
  }

  public static JavaSparkContext localContext(String appName) {
    SparkConf conf = new SparkConf()
        .setAppName(appName)
        .setIfMissing("spark.master", "local[*]")
        .set("spark.ui.enabled", "false");
    return new JavaSparkContext(conf);
  }
}