package org.dataalgorithms.chap06.memorysort;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
import org.dataalgorithms.util.HadoopUtil;
import org.dataalgorithms.chap06.TimeSeriesData;

/**
 * MapReduce job for moving averages of time series data 
 * by using in memory sort (without secondary sort).
 *
 * @author Mahmoud Parsian
 *
 */
public class SortInMemory_MovingAverageDriver {
 
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (otherArgs.length != 3) {
          System.err.println("Usage: SortInMemory_MovingAverageDriver <window_size> <input> <output>");
          System.exit(1);
       }
       System.out.println("args[0]: <window_size>="+otherArgs[0]);
       System.out.println("args[1]: <input>="+otherArgs[1]);
       System.out.println("args[2]: <output>="+otherArgs[2]);
       
       Job job = new Job(conf, "SortInMemory_MovingAverageDriver");

       // add jars to distributed cache
       HadoopUtil.addJarsToDistributedCache(job, "/lib/");
       
       // set mapper/reducer
       job.setMapperClass(SortInMemory_MovingAverageMapper.class);
       job.setReducerClass(SortInMemory_MovingAverageReducer.class);
       
       // define mapper's output key-value
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(TimeSeriesData.class);
              
       // define reducer's output key-value
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       
       // set window size for moving average calculation
       int windowSize = Integer.parseInt(otherArgs[0]);
       job.getConfiguration().setInt("moving.average.window.size", windowSize);      
       
       // define I/O
       FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
       
       job.setInputFormatClass(TextInputFormat.class); 
       job.setOutputFormatClass(TextOutputFormat.class);
       
       System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
