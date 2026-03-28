package ds200.lab01.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Task 1 driver: chains two MapReduce jobs.
 *
 * <p><b>Job 1</b> — Shuffled key is {@code MovieID}. Mapper emits rating per line; reducer outputs
 * {@code MovieID\tsum\tcount} for stage 2.
 *
 * <p><b>Job 2</b> — Distributed cache loads {@code movies.txt}. Mapper attaches title and emits a
 * single constant key so <b>one reducer</b> receives every movie; {@link Task1ReportReducer} sorts
 * by title, writes all detail lines, then uses {@code cleanup()} for the global “best movie (≥ 5
 * ratings)” line.
 *
 * <p>Local mode is enabled here for laptop runs ({@code mapreduce.framework.name=local},
 * {@code fs.defaultFS=file:///}). Override for YARN/HDFS deployments.
 *
 * <p>Args: {@code <merged_ratings_path> <movies.txt_path> <work_dir> <final_report.txt>}
 */
public final class Task1Driver {

  private Task1Driver() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println(
          "Usage: Task1Driver <merged_ratings> <movies.txt> <work_dir> <final_report.txt>");
      System.exit(1);
    }

    Path mergedRatings = new Path(args[0]);
    Path moviesFile = new Path(args[1]);
    Path workDir = new Path(args[2]);
    Path finalReport = new Path(args[3]);

    Configuration conf = new Configuration();
    // Local JobRunner: no YARN needed for grading on a single machine.
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");

    Path stage1 = new Path(workDir, "t1_stage1");
    Path stage2 = new Path(workDir, "t1_stage2");

    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
    fs.delete(stage1, true);
    fs.delete(stage2, true);
    if (fs.exists(finalReport)) {
      fs.delete(finalReport, false);
    }

    // --- Job 1: rating lines -> per-MovieID sum and count ---
    Job job1 = Job.getInstance(conf, "Lab01 Task1 Stage1 aggregate");
    job1.setJarByClass(Task1Driver.class);
    job1.setMapperClass(Task1RatingsMapper.class);
    job1.setReducerClass(Task1RatingsReducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    // Single reducer => one part-r-00000 for a simple copy to the final report path.
    job1.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job1, mergedRatings);
    FileOutputFormat.setOutputPath(job1, stage1);
    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }

    // --- Job 2: join titles, format lines, global max in reducer cleanup ---
    Job job2 = Job.getInstance(conf, "Lab01 Task1 Stage2 report");
    // Avoid a leading tab when the key is NullWritable.
    job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");
    job2.addCacheFile(moviesFile.toUri());
    job2.setJarByClass(Task1Driver.class);
    job2.setMapperClass(Task1ReportMapper.class);
    job2.setReducerClass(Task1ReportReducer.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(NullWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job2, stage1);
    FileOutputFormat.setOutputPath(job2, stage2);
    job2.setOutputFormatClass(TextOutputFormat.class);
    if (!job2.waitForCompletion(true)) {
      System.exit(1);
    }

    // Flatten default MR output directory to a single assignment-style text file.
    Path part = new Path(stage2, "part-r-00000");
    if (!FileUtil.copy(fs, part, fs, finalReport, false, true, conf)) {
      System.err.println("Failed to copy MR output to " + finalReport);
      System.exit(1);
    }
  }
}
