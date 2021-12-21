package org.dataalgorithms.chap28.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import edu.umd.cloud9.io.pair.PairOfLongInt;
/*
 * PairOfLongInt = Tuple2<Long, Integer>
 * PairOfLongInt.getLeftElement() returns Long
 * PairOfLongInt.getRightElement() returns Integer
 *
 */


import org.dataalgorithms.util.HadoopUtil;

/**
 * This is a driver class, to submit a Hadoop job for a monodic MapReduce algorithm.
 *
 * @author Mahmoud Parsian
 *
 */
public class MeanDriver {
 
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (otherArgs.length != 2) {
          System.err.println("Usage: MeanDriver <input> <output>");
          System.exit(1);
       }
       
       // define I/O paths
       Path inputPath = new Path(otherArgs[0]);
       Path outputPath = new Path(otherArgs[1]);
       
       Job job = new Job(conf, "MeanDriver");
       // add jars to distributed cache
       HadoopUtil.addJarsToDistributedCache(job, "/lib/");
       
       // set mapper/combiner/reducer
       job.setMapperClass(MeanMonoidizedMapper.class);
       job.setCombinerClass(MeanMonoidizedCombiner.class);
       job.setReducerClass(MeanMonoidizedReducer.class);
       
       // define mappers output key-value
       job.setMapOutputKeyClass(Text.class);
       // PairOfLongInt is a pair of (long, int)
       job.setMapOutputValueClass(PairOfLongInt.class);
              
       // define reducer's output key-value
       // define output key-value
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(DoubleWritable.class);
       
       // set I/O
       FileInputFormat.addInputPath(job, inputPath);
       FileOutputFormat.setOutputPath(job, outputPath);
       
       // define the format of I/O
       job.setInputFormatClass(SequenceFileInputFormat.class); 
       job.setOutputFormatClass(SequenceFileOutputFormat.class);
       
       // run job and return the status
       System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}