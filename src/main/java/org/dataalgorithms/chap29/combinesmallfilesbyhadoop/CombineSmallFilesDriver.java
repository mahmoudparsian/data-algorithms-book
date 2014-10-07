package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

import java.io.IOException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.dataalgorithms.util.HadoopUtil;

/**
 *  A driver class to testing combining/merging small files into big ines.
 *
 * @author Mahmoud Parsian
 *
 */
public class CombineSmallFilesDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
	 long beginTime = System.currentTimeMillis();
     System.exit(ToolRunner.run(new Configuration(), new CombineSmallFilesDriver(), args));
     long elapsedTime = System.currentTimeMillis() - beginTime;
	 System.out.println("elapsed time(millis): "+ elapsedTime);      
  }

  @Override
  public int run(String[] args) throws Exception {
     System.out.println("input path = "+ args[0]);
     System.out.println("output path = "+ args[1]);
     
     Configuration conf = getConf();
     Job job = new Job(conf);
     job.setJobName("CombineSmallFilesDriver");
     
     // add jars in hdfs:///lib/*.jar to Hadoop's DistributedCache
     // we place all our jars into HDFS's /lib/ directory
     HadoopUtil.addJarsToDistributedCache(job, "/lib/");     
    
     // define your custom plugin class for input format
     job.setInputFormatClass(CustomCFIF.class);
    
     // define mapper's output (K,V)
     job.setMapOutputKeyClass(Text.class);
     job.setMapOutputValueClass(IntWritable.class);
    
     // define map() and reduce() functions
     job.setMapperClass(WordCountMapper.class);
     job.setReducerClass(WordCountReducer.class);
     //job.setNumReduceTasks(13);
    
     // define I/O
     Path inputPath = new Path(args[0]);
     Path outputPath = new Path(args[1]);
     FileInputFormat.addInputPath(job, inputPath);
     FileOutputFormat.setOutputPath(job, outputPath);
    
     // submit job and wait for its completion
     job.submit();
     job.waitForCompletion(true);
     return 0;
  }
  
}
