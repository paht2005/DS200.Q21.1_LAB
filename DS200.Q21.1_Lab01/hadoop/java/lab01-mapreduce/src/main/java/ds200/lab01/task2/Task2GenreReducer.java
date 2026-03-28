package ds200.lab01.task2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import ds200.lab01.Lab01Parse;

/** Average rating and total count per genre (one report line per genre, same wording as the Python reducer). */
public class Task2GenreReducer extends Reducer<Text, Text, NullWritable, Text> {

  private final Text line = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0.0;
    long count = 0;
    for (Text t : values) {
      try {
        sum += Double.parseDouble(t.toString());
        count++;
      } catch (NumberFormatException ignored) {
        // Skip non-numeric rating tokens.
      }
    }
    if (count <= 0) {
      return;
    }
    double avg = sum / count;
    line.set(
        key.toString()
            + ": "
            + Lab01Parse.fmtRating(avg)
            + " (TotalRatings: "
            + count
            + ")");
    context.write(NullWritable.get(), line);
  }
}
