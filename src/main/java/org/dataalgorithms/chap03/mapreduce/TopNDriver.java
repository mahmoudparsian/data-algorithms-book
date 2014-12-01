package org.dataalgorithms.chap03.mapreduce;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.dataalgorithms.util.HadoopUtil;

/**
 * TopNDriver: assumes that all K's are unique for all given (K,V) values.
 * Uniqueness of keys can be achieved by using AggregateByKeyDriver job.
 *
 * @author Mahmoud Parsian
 *
 */
public class TopNDriver  extends Configured implements Tool {

   private static Logger THE_LOGGER = Logger.getLogger(TopNDriver.class);

   public int run(String[] args) throws Exception {
      Job job = new Job(getConf());
      HadoopUtil.addJarsToDistributedCache(job, "/lib/");
      int N = Integer.parseInt(args[0]); // top N
      job.getConfiguration().setInt("N", N);
      job.setJobName("TopNDriver");

      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setMapperClass(TopNMapper.class);
      job.setReducerClass(TopNReducer.class);
      job.setNumReduceTasks(1);

      // map()'s output (K,V)
      job.setMapOutputKeyClass(NullWritable.class);   
      job.setMapOutputValueClass(Text.class);   
      
      // reduce()'s output (K,V)
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);

       // args[1] = input directory
       // args[2] = output directory
      FileInputFormat.setInputPaths(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      boolean status = job.waitForCompletion(true);
      THE_LOGGER.info("run(): status="+status);
      return status ? 0 : 1;
   }

   /**
   * The main driver for "Top N" program.
   * Invoke this method to submit the map/reduce job.
   * @throws Exception When there is communication problems with the job tracker.
   */
   public static void main(String[] args) throws Exception {
      // Make sure there are exactly 3 parameters
      if (args.length != 3) {
         THE_LOGGER.warn("usage TopNDriver <N> <input> <output>");
         System.exit(1);
      }

      THE_LOGGER.info("N="+args[0]);
      THE_LOGGER.info("inputDir="+args[1]);
      THE_LOGGER.info("outputDir="+args[2]);
      int returnStatus = ToolRunner.run(new TopNDriver(), args);
      System.exit(returnStatus);
   }

}