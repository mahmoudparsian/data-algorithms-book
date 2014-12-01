package org.dataalgorithms.chap03.mapreduce;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.dataalgorithms.util.HadoopUtil;

/**
 * This class, AggregateByKeyDriver, aggregates/sums all the values for 
 * a given key.  The goal is to generate unique set of keys. For example, 
 * if input is (K,2), (K,3)  then output will be (K, 5).
 *
 * Output of this job will be used as an input to TopNDriver, which assumes 
 * that all given keys are unique.
 *
 * @author Mahmoud Parsian
 *
 */
public class AggregateByKeyDriver  extends Configured implements Tool {

   private static Logger THE_LOGGER = Logger.getLogger(AggregateByKeyDriver.class);

   public int run(String[] args) throws Exception {
      Job job = new Job(getConf());
      HadoopUtil.addJarsToDistributedCache(job, "/lib/");
      job.setJobName("AggregateByKeyDriver");

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(AggregateByKeyMapper.class);
      job.setReducerClass(AggregateByKeyReducer.class);
      job.setCombinerClass(AggregateByKeyReducer.class);

       // args[0] = input directory
       // args[1] = output directory
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      boolean status = job.waitForCompletion(true);
      THE_LOGGER.info("run(): status="+status);
      return status ? 0 : 1;
   }

   /**
   * The main driver for "Aggregate By Key" program.
   * Invoke this method to submit the map/reduce job.
   * @throws Exception When there is communication problems with the job tracker.
   */
   public static void main(String[] args) throws Exception {
      // Make sure there are exactly 2 parameters
      if (args.length != 2) {
         THE_LOGGER.warn("usage AggregateByKeyDriver <input> <output>");
         System.exit(1);
      }

      THE_LOGGER.info("inputDir="+args[0]);
      THE_LOGGER.info("outputDir="+args[1]);
      int returnStatus = ToolRunner.run(new AggregateByKeyDriver(), args);
      System.exit(returnStatus);
   }

}