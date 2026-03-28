package ds200.lab01.task1;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ds200.lab01.SideTables;

/**
 * Join stage-1 aggregates with movie titles (movies.txt via distributed cache).
 * Emits a single reduce key so one reducer can format the full report (cleanup-style summary).
 */
public class Task1ReportMapper extends Mapper<LongWritable, Text, Text, Text> {

  static final String REPORT_KEY = "TASK1_REPORT";
  private static final String MOVIES_FILE = "movies.txt";

  private Map<String, String> movieTitles;

  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void setup(Context context) throws IOException {
    URI[] files = context.getCacheFiles();
    movieTitles = SideTables.loadMovieTitles(context.getConfiguration(), files, MOVIES_FILE);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.isEmpty()) {
      return;
    }
    String[] parts = line.split("\t");
    if (parts.length < 3) {
      return;
    }
    String mid = parts[0];
    double sum;
    long cnt;
    try {
      sum = Double.parseDouble(parts[1]);
      cnt = Long.parseLong(parts[2]);
    } catch (NumberFormatException e) {
      return;
    }
    if (cnt <= 0) {
      return;
    }
    double avg = sum / cnt;
    String title = movieTitles.getOrDefault(mid, mid);
    outKey.set(REPORT_KEY);
    outVal.set(title + "\t" + avg + "\t" + cnt);
    context.write(outKey, outVal);
  }
}
