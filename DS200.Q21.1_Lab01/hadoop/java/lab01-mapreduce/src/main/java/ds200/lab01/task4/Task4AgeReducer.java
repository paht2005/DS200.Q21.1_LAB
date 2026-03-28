package ds200.lab01.task4;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ds200.lab01.Lab01Parse;
import ds200.lab01.SideTables;

/** Mean rating per age bucket per movie (fixed bucket order matches the assignment). */
public class Task4AgeReducer extends Reducer<Text, Text, NullWritable, Text> {

  private static final String MOVIES_FILE = "movies.txt";
  private static final String[] BUCKET_ORDER = {"0-18", "18-35", "35-50", "50+"};

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
    Map<String, Double> sums = new LinkedHashMap<>();
    Map<String, Long> counts = new LinkedHashMap<>();
    for (String b : BUCKET_ORDER) {
      sums.put(b, 0.0);
      counts.put(b, 0L);
    }
    for (Text t : values) {
      String[] parts = t.toString().split("\t", 2);
      if (parts.length < 2) {
        continue;
      }
      String bucket = parts[0];
      if (!sums.containsKey(bucket)) {
        continue;
      }
      double r;
      try {
        r = Double.parseDouble(parts[1]);
      } catch (NumberFormatException e) {
        continue;
      }
      sums.put(bucket, sums.get(bucket) + r);
      counts.put(bucket, counts.get(bucket) + 1);
    }
    String mid = key.toString();
    String title = titles.getOrDefault(mid, mid);
    StringBuilder sb = new StringBuilder();
    sb.append(title).append(": [");
    for (int i = 0; i < BUCKET_ORDER.length; i++) {
      String b = BUCKET_ORDER[i];
      long c = counts.get(b);
      if (i > 0) {
        sb.append(", ");
      }
      if (c == 0) {
        sb.append(b).append(": N/A");
      } else {
        sb.append(b).append(": ").append(Lab01Parse.fmtRating(sums.get(b) / c));
      }
    }
    sb.append("]");
    line.set(sb.toString());
    context.write(NullWritable.get(), line);
  }
}
