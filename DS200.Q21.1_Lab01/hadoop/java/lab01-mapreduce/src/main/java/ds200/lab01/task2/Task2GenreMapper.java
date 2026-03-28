package ds200.lab01.task2;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ds200.lab01.Lab01Parse;
import ds200.lab01.SideTables;

/**
 * Replicated join: explode each rating to one intermediate record per genre (Genres split on {@code |}).
 */
public class Task2GenreMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static final String MOVIES_FILE = "movies.txt";

  private Map<String, List<String>> genresByMovie;

  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void setup(Context context) throws IOException {
    URI[] files = context.getCacheFiles();
    genresByMovie = SideTables.loadGenresByMovie(context.getConfiguration(), files, MOVIES_FILE);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] p = Lab01Parse.parseRatingLine(value.toString());
    if (p == null) {
      return;
    }
    String mid = p[1];
    List<String> genres = genresByMovie.get(mid);
    if (genres == null || genres.isEmpty()) {
      return;
    }
    for (String g : genres) {
      outKey.set(g);
      outVal.set(p[2]);
      context.write(outKey, outVal);
    }
  }
}
