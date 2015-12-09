package org.dataalgorithms.chap07.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.log4j.Logger;
import org.dataalgorithms.util.HadoopUtil;

/**
 * This is the driver class to submit the MBA job.
 *
 * Market Basket Analysis Algorithm: find the association rule for the list of items. 
 * in a basket; That is, there are transaction data in a store
 * <ul>
 * <li>Transaction 1: steak, corn, cracker, coke, honey </li>
 * <l1>Transaction 2: icecream, apple, bread, soap</li>
 * <li>...</li>
 * <ul>
 * 
 * <p>
 * The code reads the data as 
 * key: first item
 * value: the rest of the items
 * 
 * 
 * @author Mahmoud Parsian
 *
 */
public class MBADriver extends Configured implements Tool {
   public static final Logger THE_LOGGER = Logger.getLogger(MBADriver.class);

   // main to start from the command
   public static void main(String args[]) throws Exception {
      if(args.length != 3){
         printUsage();
         System.exit(1);
      }

      int exitStatus = ToolRunner.run(new MBADriver(), args);
      THE_LOGGER.info("exitStatus="+exitStatus);
      System.exit(exitStatus);
   }      
   
   private static int printUsage(){
      System.out.println("USAGE: [input-path] [output-path] [number-of-pairs]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
   }


   @Override
   public int run(String args[]) throws Exception {
      String inputPath = args[0];
      String outputPath = args[1];
      int numberOfPairs = Integer.parseInt(args[2]);
      
      THE_LOGGER.info("inputPath: " + inputPath);
      THE_LOGGER.info("outputPath: " + outputPath);
      THE_LOGGER.info("numberOfPairs: " + numberOfPairs);
      
      // job configuration
      Job job = new Job(getConf());
      job.setJobName("MBADriver");
      job.getConfiguration().setInt("number.of.pairs", numberOfPairs);   
      		
      // job.setJarByClass(MBADriver.class);
      // add jars to distributed cache
      HadoopUtil.addJarsToDistributedCache(job, "/lib/");


      //input/output path
      FileInputFormat.setInputPaths(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));      

      //Mapper K, V output
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);   
      //output format
      job.setOutputFormatClass(TextOutputFormat.class);
      
      //Reducer K, V output
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      
      // set mapper/reducer
      job.setMapperClass(MBAMapper.class);
      job.setCombinerClass(MBAReducer.class);
      job.setReducerClass(MBAReducer.class);
      
      //delete the output path if exists to avoid "existing dir/file" error
      Path outputDir = new Path(outputPath);
      FileSystem.get(getConf()).delete(outputDir, true);
      
      long startTime = System.currentTimeMillis();
      boolean status = job.waitForCompletion(true);
      THE_LOGGER.info("job status="+status);
      long endTime = System.currentTimeMillis();
      long elapsedTime =  endTime - startTime;
      THE_LOGGER.info("Elapsed time: " + elapsedTime + " milliseconds");
   
      return status ? 0 : 1;      
   }

}
