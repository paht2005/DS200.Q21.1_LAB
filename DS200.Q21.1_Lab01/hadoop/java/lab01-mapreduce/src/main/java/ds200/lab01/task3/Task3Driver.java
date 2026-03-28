package ds200.lab01.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task 3 driver: one MapReduce job with two cached side files.
 *
 * <p>Mapper joins each rating to {@code users.txt} on {@code UserID} and emits
 * {@code (MovieID, Gender\tRating)} for {@code M}/{@code F} only. Reducer loads {@code movies.txt} to
 * print human-readable titles and separate male/female averages. Post-run {@code sort} in the shell
 * script matches the Python pipeline’s alphabetical ordering by line.
 *
 * <p>Args: {@code <merged_ratings> <users.txt> <movies.txt> <work_dir> <final_report.txt>}
 */
public final class Task3Driver {

  private Task3Driver() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println(
          "Usage: Task3Driver <merged_ratings> <users.txt> <movies.txt> <work_dir> <final_report.txt>");
      System.exit(1);
    }

    Path mergedRatings = new Path(args[0]);
    Path usersFile = new Path(args[1]);
    Path moviesFile = new Path(args[2]);
    Path workDir = new Path(args[3]);
    Path finalReport = new Path(args[4]);

    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///"); // laptop-friendly; use HDFS + YARN on a cluster

    Path outDir = new Path(workDir, "t3_out");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
    fs.delete(outDir, true);
    if (fs.exists(finalReport)) {
      fs.delete(finalReport, false);
    }

    Job job = Job.getInstance(conf, "Lab01 Task3 gender by movie");
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
    job.addCacheFile(usersFile.toUri());
    job.addCacheFile(moviesFile.toUri());
    job.setJarByClass(Task3Driver.class);
    job.setMapperClass(Task3GenderMapper.class);
    job.setReducerClass(Task3GenderReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, mergedRatings);
    FileOutputFormat.setOutputPath(job, outDir);
    if (!job.waitForCompletion(true)) {
      System.exit(1);
    }

    Path part = new Path(outDir, "part-r-00000");
    if (!org.apache.hadoop.fs.FileUtil.copy(fs, part, fs, finalReport, false, true, conf)) {
      System.err.println("Failed to copy MR output to " + finalReport);
      System.exit(1);
    }
  }
}
