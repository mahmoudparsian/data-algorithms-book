package org.dataalgorithms.chapB05.anagram.mapreduce;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * AnagramDriver: submits the job to Hadoop to find and count anagrams
 *
 * @author Mahmoud Parsian
 *
 */
public class AnagramDriver  extends Configured implements Tool {

   private static Logger theLogger = Logger.getLogger(AnagramDriver.class);

   @Override
   public int run(String[] args) throws Exception {
          
      Job job = new Job(getConf());
      job.setJarByClass(AnagramDriver.class);
      job.setJobName("AnagramDriver");
      
      int N = Integer.parseInt(args[0]);
      job.getConfiguration().setInt("word.count.ignored.length", N);   
         
      job.setInputFormatClass(TextInputFormat.class); 
      job.setOutputFormatClass(TextOutputFormat.class);
      
      job.setOutputKeyClass(Text.class);         
      job.setOutputValueClass(Text.class);    
          
      job.setMapperClass(AnagramMapper.class);
      job.setReducerClass(AnagramReducer.class);
      
       // args[1] = input directory
       // args[2] = output directory
      FileInputFormat.setInputPaths(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      boolean status = job.waitForCompletion(true);
      theLogger.info("run(): status="+status);
      return status ? 0 : 1;
   }

   /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws Exception When there is communication problems with the job tracker.
   */
   public static void main(String[] args) throws Exception {
      // Make sure there are exactly 3 parameters
      if (args.length != 3) {
         throw new IllegalArgumentException("usage: <N> <input> <output>");
      }

      //String N = args[0];
      theLogger.info("N="+args[0]);
      
      //String inputDir = args[1];
      theLogger.info("inputDir="+args[1]);

      //String outputDir = args[2];
      theLogger.info("outputDir="+args[2]);

      int returnStatus = submitJob(args);
      theLogger.info("returnStatus="+returnStatus);
      
      System.exit(returnStatus);
   }


   /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws Exception When there is communication problems with the job tracker.
   */
   public static int submitJob(String[] args) throws Exception {
      int returnStatus = ToolRunner.run(new AnagramDriver(), args);
      return returnStatus;
   }
}
