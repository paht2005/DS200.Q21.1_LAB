package ds200.lab01.task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ds200.lab01.Lab01Parse;

/**
 * Single-key reducer group for Task 1 stage 2 (key {@code TASK1_REPORT}).
 *
 * <p>{@link #reduce} collects every {@code title\tavg\tcount} value into {@link #rows}. {@link
 * #cleanup} then sorts by title, writes all per-movie lines, a blank line, and finally either the
 * global maximum-average line among movies with at least {@link #MIN_RATINGS} ratings or the
 * assignment’s fallback sentence — mirroring a “cleanup after all keys” teaching pattern.
 */
public class Task1ReportReducer extends Reducer<Text, Text, NullWritable, Text> {

  static final int MIN_RATINGS = 5;

  private static final class Row {
    final String title;
    final double avg;
    final long count;

    Row(String title, double avg, long count) {
      this.title = title;
      this.avg = avg;
      this.count = count;
    }
  }

  /** In-memory list for the one reduce call for key {@code TASK1_REPORT}; emitted from {@link #cleanup}. */
  private final List<Row> rows = new ArrayList<>();

  private final Text line = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) {
    rows.clear();
    for (Text t : values) {
      String s = t.toString();
      String[] p = s.split("\t", 3);
      if (p.length < 3) {
        continue;
      }
      try {
        double avg = Double.parseDouble(p[1]);
        long cnt = Long.parseLong(p[2]);
        rows.add(new Row(p[0], avg, cnt));
      } catch (NumberFormatException ignored) {
        // Ignore malformed numeric fields on this line.
      }
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Collections.sort(rows, Comparator.comparing(r -> r.title));
    for (Row r : rows) {
      line.set(
          r.title
              + " AverageRating: "
              + Lab01Parse.fmtRating(r.avg)
              + " (TotalRatings: "
              + r.count
              + ")");
      context.write(NullWritable.get(), line);
    }
    line.set("");
    context.write(NullWritable.get(), line);

    Row best = null;
    for (Row r : rows) {
      if (r.count < MIN_RATINGS) {
        continue;
      }
      if (best == null || r.avg > best.avg) {
        best = r;
      }
    }
    if (best == null) {
      line.set("No movie has at least 5 ratings.");
    } else {
      line.set(
          best.title
              + " is the highest rated movie with an average rating of "
              + Lab01Parse.fmtRating(best.avg)
              + " among movies with at least 5 ratings.");
    }
    context.write(NullWritable.get(), line);
  }
}
