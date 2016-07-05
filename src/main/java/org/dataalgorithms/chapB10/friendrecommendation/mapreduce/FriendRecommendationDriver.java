package org.dataalgorithms.chapB10.friendrecommendation.mapreduce;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Description: 
 *
 *     Find friends recommendation using 2-phase MapReduce programs.
 *
 *     FriendRecommendationDriver submits MR-phase1 followed 
 *     by MR-phase2: to find friend recommendations.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class FriendRecommendationDriver  extends Configured implements Tool {

    private static Logger theLogger = Logger.getLogger(FriendRecommendationDriver.class);

    public int run(String[] args) throws Exception {
    
       int numberOfRecommendations = Integer.parseInt(args[0]);
       String inputPath = args[1];  // hdfs path as a string
       String outputPath = args[2]; // hdfs path as a string

       // phase1:
       Job job1 = new Job(getConf(), "job1");
       job1.setJarByClass(FriendRecommendationDriver.class);
        
       // mapper's output (K,V) classes
       job1.setMapOutputKeyClass(PairOfLongs.class);
       job1.setMapOutputValueClass(LongWritable.class);
        
       // reducer's output (K,V) classes
       job1.setOutputKeyClass(PairOfLongs.class);
       job1.setOutputValueClass(LongWritable.class);
    
       // identify map() and reduce() functions
       job1.setMapperClass(Phase1Mapper.class);
       job1.setReducerClass(Phase1Reducer.class);
    
       // identify format of I/O paths
       job1.setInputFormatClass(TextInputFormat.class);
       // create a SequenceFile, so we do not have to do further parsing
       job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    
       FileInputFormat.setInputPaths(job1, new Path(inputPath));
       FileOutputFormat.setOutputPath(job1, new Path("/tmp/intermediate"));
       job1.waitForCompletion(true);


       // phase2:
       Job job2 = new Job(getConf(), "job2");
       job2.setJarByClass(FriendRecommendationDriver.class);

       // the number of recommendations for a user to be generated
       job2.getConfiguration().setInt("number.of.recommendations", numberOfRecommendations);
        
       // mapper's output (K,V) classes
       job2.setMapOutputKeyClass(LongWritable.class);
       job2.setMapOutputValueClass(PairOfLongs.class);

       // reducer's output (K,V) classes
       job2.setOutputKeyClass(LongWritable.class);
       job2.setOutputValueClass(Text.class);
    
    
       // identify map() and reduce() functions
       job2.setMapperClass(Phase2Mapper.class);
       job2.setReducerClass(Phase2Reducer.class);
    
       // read from a SequenceFile, so we do not have to do further parsing
       job2.setInputFormatClass(SequenceFileInputFormat.class);
       job2.setOutputFormatClass(TextOutputFormat.class);
    
       FileInputFormat.setInputPaths(job2, new Path("/tmp/intermediate"));
       FileOutputFormat.setOutputPath(job2, new Path(outputPath)); 
       boolean status2 = job2.waitForCompletion(true);
       theLogger.info("run2(): status2="+status2);
       return status2 ? 0 : 1;
    }

    /**
     * The main driver for word count map/reduce program.
     * Invoke this method to submit the map/reduce job.
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static void main(String[] args) throws Exception {
       // Make sure there are exactly 2 parameters
       if (args.length != 3) {
          throw new IllegalArgumentException("usage: <number-of-recommendations> <input-path> <output-path>");
       }

       //String number.of.recommendations = args[0];
       theLogger.info("number.of.recommendations="+args[0]);
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
       //     args[0] = number.of.recommendations
       //     args[1] = input path
       //     args[2] = output path
       int returnStatus = ToolRunner.run(new FriendRecommendationDriver(), args);
       return returnStatus;
    }
}

