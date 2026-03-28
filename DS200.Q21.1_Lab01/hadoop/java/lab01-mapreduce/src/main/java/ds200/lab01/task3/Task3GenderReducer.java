package ds200.lab01.task3;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ds200.lab01.Lab01Parse;
import ds200.lab01.SideTables;

/** Per MovieID: male vs female mean rating; resolve title via movies.txt (distributed cache). */
public class Task3GenderReducer extends Reducer<Text, Text, NullWritable, Text> {

  private static final String MOVIES_FILE = "movies.txt";

  private Map<String, String> titles;

  private final Text line = new Text();

  @Override
  protected void setup(Context context) throws IOException {
    URI[] files = context.getCacheFiles();
    titles = SideTables.loadMovieTitles(context.getConfiguration(), files, MOVIES_FILE);
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    double sumM = 0.0;
    double sumF = 0.0;
    long cntM = 0;
    long cntF = 0;
    for (Text t : values) {
      String[] parts = t.toString().split("\t", 2);
      if (parts.length < 2) {
        continue;
      }
      String g = parts[0];
      double r;
      try {
        r = Double.parseDouble(parts[1]);
      } catch (NumberFormatException e) {
        continue;
      }
      if ("M".equals(g)) {
        sumM += r;
        cntM++;
      } else if ("F".equals(g)) {
        sumF += r;
        cntF++;
      }
    }
    String mid = key.toString();
    String title = titles.getOrDefault(mid, mid);
    String maleS = cntM == 0 ? "N/A" : Lab01Parse.fmtRating(sumM / cntM);
    String femaleS = cntF == 0 ? "N/A" : Lab01Parse.fmtRating(sumF / cntF);
    line.set(title + ": " + maleS + ", " + femaleS);
    context.write(NullWritable.get(), line);
  }
}
