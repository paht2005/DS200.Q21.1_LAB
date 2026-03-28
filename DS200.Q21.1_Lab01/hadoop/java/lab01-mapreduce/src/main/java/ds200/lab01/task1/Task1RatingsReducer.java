package ds200.lab01.task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Sum ratings and count rows per MovieID (shuffle groups by MovieID).
 * Output: MovieID TAB sum TAB count (for stage-2 join with titles).
 */
public class Task1RatingsReducer extends Reducer<Text, Text, Text, Text> {

  private final Text outVal = new Text();

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
        // Skip malformed rating values in the iterable.
      }
    }
    if (count <= 0) {
      return;
    }
    outVal.set(sum + "\t" + count);
    context.write(key, outVal);
  }
}
