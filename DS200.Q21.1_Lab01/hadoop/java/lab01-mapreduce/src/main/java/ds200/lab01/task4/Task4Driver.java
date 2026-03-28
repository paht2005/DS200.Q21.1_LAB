package ds200.lab01.task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task 4 driver: one MapReduce job; same cache split as Task 3.
 *
 * <p>Mapper joins ratings to users, buckets age with {@link ds200.lab01.Lab01Parse#ageBucket(int)},
 * and emits {@code (MovieID, Bucket\tRating)}. Reducer aggregates four fixed buckets and resolves
 * titles from {@code movies.txt}.
 *
 * <p>Args: {@code <merged_ratings> <users.txt> <movies.txt> <work_dir> <final_report.txt>}
 */
public final class Task4Driver {

  private Task4Driver() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println(
          "Usage: Task4Driver <merged_ratings> <users.txt> <movies.txt> <work_dir> <final_report.txt>");
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

    Path outDir = new Path(workDir, "t4_out");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
    fs.delete(outDir, true);
    if (fs.exists(finalReport)) {
      fs.delete(finalReport, false);
    }

    Job job = Job.getInstance(conf, "Lab01 Task4 age groups by movie");
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
    job.addCacheFile(usersFile.toUri());
    job.addCacheFile(moviesFile.toUri());
    job.setJarByClass(Task4Driver.class);
    job.setMapperClass(Task4AgeMapper.class);
    job.setReducerClass(Task4AgeReducer.class);
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
