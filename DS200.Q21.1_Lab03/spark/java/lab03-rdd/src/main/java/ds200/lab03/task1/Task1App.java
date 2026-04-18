package ds200.lab03.task1;

import ds200.lab03.model.Movie;
import ds200.lab03.model.Rating;
import ds200.lab03.model.RatingStats;
import ds200.lab03.util.Lab03Parse;
import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class Task1App {
  private Task1App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      throw new IllegalArgumentException(
          "Usage: Task1App <moviesPath> <ratings1Path> <ratings2Path> <outputFile> <minRatings>");
    }

    String moviesPath = args[0];
    String ratings1Path = args[1];
    String ratings2Path = args[2];
    String outputFile = args[3];
    long minRatings = Long.parseLong(args[4]);

    try (JavaSparkContext sc = SparkContexts.localContext("DS200-Lab03-Task1")) {
      Map<Integer, Movie> movieById = sc.textFile(moviesPath)
          .map(Lab03Parse::parseMovie)
          .filter(movie -> movie != null)
          .mapToPair(movie -> new Tuple2<>(movie.getMovieId(), movie))
          .collectAsMap();

      JavaRDD<Rating> ratings = sc.textFile(ratings1Path)
          .union(sc.textFile(ratings2Path))
          .map(Lab03Parse::parseRating)
          .filter(rating -> rating != null);

      List<Tuple2<Integer, RatingStats>> reduced = new ArrayList<>(ratings
          .mapToPair(rating -> new Tuple2<>(rating.getMovieId(), new RatingStats(rating.getRating(), 1)))
          .reduceByKey(RatingStats::merge)
          .collect());

      reduced.sort(
          Comparator.comparing((Tuple2<Integer, RatingStats> t) -> movieById.containsKey(t._1)
                  ? movieById.get(t._1).getTitle()
                  : String.valueOf(t._1))
              .thenComparing(Tuple2::_1)
      );

      List<String> lines = new ArrayList<>();
      lines.add("MovieID|Title|AverageRating|TotalRatings");

      Tuple2<Integer, RatingStats> topMovie = null;
      for (Tuple2<Integer, RatingStats> entry : reduced) {
        int movieId = entry._1;
        RatingStats stats = entry._2;
        String title = movieById.containsKey(movieId) ? movieById.get(movieId).getTitle() : "Unknown Movie";
        lines.add(movieId + "|" + title + "|" + Lab03Parse.fmt(stats.getAverage()) + "|" + stats.getCount());

        if (stats.getCount() >= minRatings) {
          if (topMovie == null) {
            topMovie = entry;
          } else {
            double currentAvg = stats.getAverage();
            double bestAvg = topMovie._2.getAverage();
            if (currentAvg > bestAvg
                || (Double.compare(currentAvg, bestAvg) == 0 && stats.getCount() > topMovie._2.getCount())) {
              topMovie = entry;
            }
          }
        }
      }

      lines.add("");
      if (topMovie == null) {
        lines.add("TOP_MOVIE(minRatings=" + minRatings + ")|N/A|No movie met the threshold");
      } else {
        int topMovieId = topMovie._1;
        RatingStats topStats = topMovie._2;
        String topTitle = movieById.containsKey(topMovieId) ? movieById.get(topMovieId).getTitle() : "Unknown Movie";
        lines.add(
            "TOP_MOVIE(minRatings=" + minRatings + ")|" + topMovieId + "|" + topTitle + "|" + Lab03Parse.fmt(
                topStats.getAverage()) + "|" + topStats.getCount());
      }

      OutputWriter.writeLines(outputFile, lines);
    }
  }
}