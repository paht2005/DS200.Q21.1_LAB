package ds200.lab01.task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ds200.lab01.Lab01Parse;

/** Emit MovieID and rating from each ratings line (both rating files can share this mapper). */
public class Task1RatingsMapper extends Mapper<LongWritable, Text, Text, Text> {

  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] p = Lab01Parse.parseRatingLine(value.toString());
    if (p == null) {
      return;
    }
    outKey.set(p[1]);
    outVal.set(p[2]);
    context.write(outKey, outVal);
  }
}
