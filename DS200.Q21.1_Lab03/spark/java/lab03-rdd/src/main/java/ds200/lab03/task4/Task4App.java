package ds200.lab03.task4;

import ds200.lab03.model.Movie;
import ds200.lab03.model.Rating;
import ds200.lab03.model.RatingStats;
import ds200.lab03.model.User;
import ds200.lab03.util.Lab03Parse;
import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class Task4App {
  private Task4App() {
  }

  private static final class ResultRow {
    private final int movieId;
    private final String title;
    private final String ageGroup;
    private final RatingStats stats;

    private ResultRow(int movieId, String title, String ageGroup, RatingStats stats) {
      this.movieId = movieId;
      this.title = title;
      this.ageGroup = ageGroup;
      this.stats = stats;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      throw new IllegalArgumentException(
          "Usage: Task4App <moviesPath> <usersPath> <ratings1Path> <ratings2Path> <outputFile>");
    }

    String moviesPath = args[0];
    String usersPath = args[1];
    String ratings1Path = args[2];
    String ratings2Path = args[3];
    String outputFile = args[4];

    try (JavaSparkContext sc = SparkContexts.localContext("DS200-Lab03-Task4")) {
      Map<Integer, Movie> movieById = sc.textFile(moviesPath)
          .map(Lab03Parse::parseMovie)
          .filter(movie -> movie != null)
          .mapToPair(movie -> new Tuple2<>(movie.getMovieId(), movie))
          .collectAsMap();

      Map<Integer, User> userById = sc.textFile(usersPath)
          .map(Lab03Parse::parseUser)
          .filter(user -> user != null)
          .mapToPair(user -> new Tuple2<>(user.getUserId(), user))
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
            String ageGroup = Lab03Parse.ageGroup(user.getAge());
            rows.add(new Tuple2<>(rating.getMovieId() + "|" + ageGroup, new RatingStats(rating.getRating(), 1)));
            return rows.iterator();
          })
          .reduceByKey(RatingStats::merge)
          .collect());

      Map<String, Integer> ageOrder = new HashMap<>();
      ageOrder.put("0-18", 0);
      ageOrder.put("19-35", 1);
      ageOrder.put("36-50", 2);
      ageOrder.put("51+", 3);

      List<ResultRow> rows = new ArrayList<>();
      for (Tuple2<String, RatingStats> entry : reduced) {
        String[] keyParts = entry._1.split("\\|", 2);
        int movieId = Integer.parseInt(keyParts[0]);
        String ageGroup = keyParts[1];
        String title = movieById.containsKey(movieId) ? movieById.get(movieId).getTitle() : "Unknown Movie";
        rows.add(new ResultRow(movieId, title, ageGroup, entry._2));
      }

      rows.sort(
          Comparator.comparing((ResultRow row) -> row.title)
              .thenComparingInt(row -> ageOrder.getOrDefault(row.ageGroup, 99))
      );

      List<String> lines = new ArrayList<>();
      lines.add("MovieID|Title|AgeGroup|AverageRating|TotalRatings");
      for (ResultRow row : rows) {
        lines.add(
            row.movieId + "|" + row.title + "|" + row.ageGroup + "|" + Lab03Parse.fmt(row.stats.getAverage()) + "|" + row.stats.getCount());
      }

      OutputWriter.writeLines(outputFile, lines);
    }
  }
}