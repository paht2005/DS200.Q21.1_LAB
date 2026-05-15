package ds200.lab04.util;

import org.apache.spark.sql.SparkSession;

public final class SparkSessions {
  private SparkSessions() {
  }

  public static SparkSession local(String appName) {
    return SparkSession.builder()
        .appName(appName)
        .config("spark.master", "local[*]")
        .config("spark.ui.enabled", "false")
        // Disable ANSI mode so invalid casts return null instead of throwing
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate();
  }
}
