package ds200.lab01.task3;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ds200.lab01.Lab01Parse;
import ds200.lab01.SideTables;
import ds200.lab01.SideTables.UserRow;

/** Join ratings with users on UserID; emit MovieID and {@code Gender\tRating}. */
public class Task3GenderMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static final String USERS_FILE = "users.txt";

  private Map<String, UserRow> users;

  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void setup(Context context) throws IOException {
    URI[] files = context.getCacheFiles();
    users = SideTables.loadUsers(context.getConfiguration(), files, USERS_FILE);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] p = Lab01Parse.parseRatingLine(value.toString());
    if (p == null) {
      return;
    }
    String uid = p[0];
    String mid = p[1];
    UserRow u = users.get(uid);
    if (u == null) {
      return;
    }
    if (!"M".equals(u.gender) && !"F".equals(u.gender)) {
      return;
    }
    outKey.set(mid);
    outVal.set(u.gender + "\t" + p[2]);
    context.write(outKey, outVal);
  }
}
