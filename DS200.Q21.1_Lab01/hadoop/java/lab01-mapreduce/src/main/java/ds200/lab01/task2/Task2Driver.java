package ds200.lab01.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Task 2 driver: one MapReduce job.
 *
 * <p>Mapper loads {@code movies.txt} from the distributed cache, splits the genre list on {@code |},
 * and emits one {@code (Genre, Rating)} pair per (rating, genre) combination. Reducer averages ratings
 * per genre. {@code setNumReduceTasks(1)} yields a single {@code part-r-00000} for the shell script
 * to copy into {@code output/task2_genre_ratings.txt}.
 *
 * <p>Args: {@code <merged_ratings> <movies.txt> <work_dir> <final_report.txt>}
 */
public final class Task2Driver {

  private Task2Driver() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println(
          "Usage: Task2Driver <merged_ratings> <movies.txt> <work_dir> <final_report.txt>");
      System.exit(1);
    }

    Path mergedRatings = new Path(args[0]);
    Path moviesFile = new Path(args[1]);
    Path workDir = new Path(args[2]);
    Path finalReport = new Path(args[3]);

    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///"); // laptop-friendly; use HDFS + YARN on a cluster

    Path outDir = new Path(workDir, "t2_out");
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
    fs.delete(outDir, true);
    if (fs.exists(finalReport)) {
      fs.delete(finalReport, false);
    }

    Job job = Job.getInstance(conf, "Lab01 Task2 genre averages");
    job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
    job.addCacheFile(moviesFile.toUri());
    job.setJarByClass(Task2Driver.class);
    job.setMapperClass(Task2GenreMapper.class);
    job.setReducerClass(Task2GenreReducer.class);
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
