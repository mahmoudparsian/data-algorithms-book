package org.dataalgorithms.chap11.projection.memorysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import org.apache.log4j.Logger;

import org.dataalgorithms.util.HadoopUtil;


/**
 * MapReduce job for projecting customer transaction data
 * by using in memory sort (without secondary sort design pattern).
 *
 * @author Mahmoud Parsian
 *
 */
public class SortInMemoryProjectionDriver {
	private static final Logger theLogger = 
	   Logger.getLogger(SortInMemoryProjectionDriver.class); 
	   
    public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();	
    
       Configuration conf = new Configuration();
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (otherArgs.length != 2) {
          theLogger.warn("Usage: SortInMemoryProjectionDriver <input> <output>");
          System.exit(1);
       }
       Job job = new Job(conf, "SortInMemoryProjectionDriver");

       // add jars to distributed cache
       HadoopUtil.addJarsToDistributedCache(job, "/lib/");
       
       // set mapper/reducer
       job.setMapperClass(SortInMemoryProjectionMapper.class);
       job.setReducerClass(SortInMemoryProjectionReducer.class);
       
       // define mapper's output key-value
       job.setMapOutputKeyClass(Text.class);
       // PairOfLongInt = pair(long, int) = pair(date, amount)
       job.setMapOutputValueClass(PairOfLongInt.class);
              
       // define reducer's output key-value
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
              
       // define I/O
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       
       job.setInputFormatClass(TextInputFormat.class); 
       job.setOutputFormatClass(TextOutputFormat.class);
 
	   boolean jobStatus = job.waitForCompletion(true);      
	   long elapsedTime = System.currentTimeMillis() - startTime;
       theLogger.info("jobStatus: "+ jobStatus);      
       theLogger.info("elapsedTime (in milliseconds): "+ elapsedTime);      
       System.exit(jobStatus? 0 : 1);
    }

}
