package ds200.lab03.task3;

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

public final class Task3App {
  private Task3App() {
  }

  private static final class GenderStats {
    private RatingStats male;
    private RatingStats female;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      throw new IllegalArgumentException(
          "Usage: Task3App <moviesPath> <usersPath> <ratings1Path> <ratings2Path> <outputFile>");
    }

    String moviesPath = args[0];
    String usersPath = args[1];
    String ratings1Path = args[2];
    String ratings2Path = args[3];
    String outputFile = args[4];

    try (JavaSparkContext sc = SparkContexts.localContext("DS200-Lab03-Task3")) {
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
            String gender = user.getGender();
            if (!"M".equals(gender) && !"F".equals(gender)) {
              return rows.iterator();
            }
            String key = rating.getMovieId() + "|" + gender;
            rows.add(new Tuple2<>(key, new RatingStats(rating.getRating(), 1)));
            return rows.iterator();
          })
          .reduceByKey(RatingStats::merge)
          .collect());

      Map<Integer, GenderStats> byMovie = new HashMap<>();
      for (Tuple2<String, RatingStats> row : reduced) {
        String[] keyParts = row._1.split("\\|", 2);
        int movieId = Integer.parseInt(keyParts[0]);
        String gender = keyParts[1];
        GenderStats gs = byMovie.computeIfAbsent(movieId, ignored -> new GenderStats());
        if ("M".equals(gender)) {
          gs.male = row._2;
        } else {
          gs.female = row._2;
        }
      }

      List<Integer> movieIds = new ArrayList<>(byMovie.keySet());
      movieIds.sort(
          Comparator.comparing((Integer movieId) -> movieById.containsKey(movieId)
                  ? movieById.get(movieId).getTitle()
                  : String.valueOf(movieId))
              .thenComparing(movieId -> movieId)
      );

      List<String> lines = new ArrayList<>();
      lines.add("MovieID|Title|MaleAverage|MaleCount|FemaleAverage|FemaleCount");
      for (Integer movieId : movieIds) {
        String title = movieById.containsKey(movieId) ? movieById.get(movieId).getTitle() : "Unknown Movie";
        GenderStats gs = byMovie.get(movieId);
        String maleAvg = gs.male == null ? "N/A" : Lab03Parse.fmt(gs.male.getAverage());
        long maleCount = gs.male == null ? 0 : gs.male.getCount();
        String femaleAvg = gs.female == null ? "N/A" : Lab03Parse.fmt(gs.female.getAverage());
        long femaleCount = gs.female == null ? 0 : gs.female.getCount();
        lines.add(movieId + "|" + title + "|" + maleAvg + "|" + maleCount + "|" + femaleAvg + "|" + femaleCount);
      }

      OutputWriter.writeLines(outputFile, lines);
    }
  }
}