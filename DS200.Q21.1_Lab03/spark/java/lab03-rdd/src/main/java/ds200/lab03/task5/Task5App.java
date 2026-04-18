package ds200.lab03.task5;

import ds200.lab03.model.Rating;
import ds200.lab03.model.RatingStats;
import ds200.lab03.model.User;
import ds200.lab03.util.Lab03Parse;
import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class Task5App {
  private Task5App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      throw new IllegalArgumentException(
          "Usage: Task5App <usersPath> <occupationsPath> <ratings1Path> <ratings2Path> <outputFile>");
    }

    String usersPath = args[0];
    String occupationsPath = args[1];
    String ratings1Path = args[2];
    String ratings2Path = args[3];
    String outputFile = args[4];

    try (JavaSparkContext sc = SparkContexts.localContext("DS200-Lab03-Task5")) {
      Map<Integer, User> userById = sc.textFile(usersPath)
          .map(Lab03Parse::parseUser)
          .filter(user -> user != null)
          .mapToPair(user -> new Tuple2<>(user.getUserId(), user))
          .collectAsMap();

      Map<Integer, String> occupationById = sc.textFile(occupationsPath)
          .map(Lab03Parse::parseOccupation)
          .filter(entry -> entry != null)
          .mapToPair(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
          .collectAsMap();

      List<Tuple2<String, RatingStats>> reduced = new ArrayList<>(sc.textFile(ratings1Path)
          .union(sc.textFile(ratings2Path))
          .map(Lab03Parse::parseRating)
          .filter(rating -> rating != null)
          .flatMapToPair((Rating rating) -> {
            List<Tuple2<String, RatingStats>> rows = new ArrayList<>();
            User user = userById.get(rating.getUserId());
            if (user == null) {
              return rows.iterator();
            }
            String occupation = occupationById.getOrDefault(
                user.getOccupationId(),
                "Occupation-" + user.getOccupationId());
            rows.add(new Tuple2<>(occupation, new RatingStats(rating.getRating(), 1)));
            return rows.iterator();
          })
          .reduceByKey(RatingStats::merge)
          .collect());

      reduced.sort(Comparator.comparing(Tuple2::_1));

      List<String> lines = new ArrayList<>();
      lines.add("Occupation|AverageRating|TotalRatings");
      for (Tuple2<String, RatingStats> entry : reduced) {
        lines.add(entry._1 + "|" + Lab03Parse.fmt(entry._2.getAverage()) + "|" + entry._2.getCount());
      }

      OutputWriter.writeLines(outputFile, lines);
    }
  }
}