package ds200.lab03.task6;

import ds200.lab03.model.Rating;
import ds200.lab03.model.RatingStats;
import ds200.lab03.util.Lab03Parse;
import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class Task6App {
  private Task6App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Usage: Task6App <ratings1Path> <ratings2Path> <outputFile>");
    }

    String ratings1Path = args[0];
    String ratings2Path = args[1];
    String outputFile = args[2];

    try (JavaSparkContext sc = SparkContexts.localContext("DS200-Lab03-Task6")) {
      List<Tuple2<Integer, RatingStats>> reduced = new ArrayList<>(sc.textFile(ratings1Path)
          .union(sc.textFile(ratings2Path))
          .map(Lab03Parse::parseRating)
          .filter(rating -> rating != null)
          .mapToPair((Rating rating) -> new Tuple2<>(Lab03Parse.yearFromTimestamp(rating.getTimestamp()),
              new RatingStats(rating.getRating(), 1)))
          .reduceByKey(RatingStats::merge)
          .collect());

      reduced.sort(Comparator.comparing(Tuple2::_1));

      List<String> lines = new ArrayList<>();
      lines.add("Year|AverageRating|TotalRatings");
      for (Tuple2<Integer, RatingStats> entry : reduced) {
        lines.add(entry._1 + "|" + Lab03Parse.fmt(entry._2.getAverage()) + "|" + entry._2.getCount());
      }

      OutputWriter.writeLines(outputFile, lines);
    }
  }
}