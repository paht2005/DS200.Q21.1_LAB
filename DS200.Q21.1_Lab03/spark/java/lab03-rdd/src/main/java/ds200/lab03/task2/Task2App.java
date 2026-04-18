package ds200.lab03.task2;

import ds200.lab03.model.RatingStats;
import ds200.lab03.util.Lab03Parse;
import ds200.lab03.util.OutputWriter;
import ds200.lab03.util.SparkContexts;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public final class Task2App {
  private Task2App() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      throw new IllegalArgumentException(
          "Usage: Task2App <moviesPath> <ratings1Path> <ratings2Path> <outputFile>");
    }

    String moviesPath = args[0];
    String ratings1Path = args[1];
    String ratings2Path = args[2];
    String outputFile = args[3];

    try (JavaSparkContext sc = SparkContexts.localContext("DS200-Lab03-Task2")) {
      Map<Integer, List<String>> genresByMovieId = sc.textFile(moviesPath)
          .map(Lab03Parse::parseMovie)
          .filter(movie -> movie != null)
          .mapToPair(movie -> new Tuple2<>(movie.getMovieId(), movie.getGenres()))
          .collectAsMap();

      List<Tuple2<String, RatingStats>> reduced = new ArrayList<>(sc.textFile(ratings1Path)
          .union(sc.textFile(ratings2Path))
          .map(Lab03Parse::parseRating)
          .filter(rating -> rating != null)
          .flatMapToPair(rating -> {
            List<Tuple2<String, RatingStats>> rows = new ArrayList<>();
            List<String> genres = genresByMovieId.get(rating.getMovieId());
            if (genres == null || genres.isEmpty()) {
              return rows.iterator();
            }
            for (String genre : genres) {
              rows.add(new Tuple2<>(genre, new RatingStats(rating.getRating(), 1)));
            }
            return rows.iterator();
          })
          .reduceByKey(RatingStats::merge)
          .collect());

      reduced.sort(Comparator.comparing(Tuple2::_1));

      List<String> lines = new ArrayList<>();
      lines.add("Genre|AverageRating|TotalRatings");
      for (Tuple2<String, RatingStats> entry : reduced) {
        lines.add(entry._1 + "|" + Lab03Parse.fmt(entry._2.getAverage()) + "|" + entry._2.getCount());
      }

      OutputWriter.writeLines(outputFile, lines);
    }
  }
}