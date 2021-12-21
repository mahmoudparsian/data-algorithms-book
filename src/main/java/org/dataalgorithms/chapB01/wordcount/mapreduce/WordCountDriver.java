package org.dataalgorithms.chapB01.wordcount.mapreduce;

import org.apache.log4j.Logger;
//
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * Description:
 *
 *    WordCountDriver: submits the job to Hadoop
 *    Counting the words if their size is greater 
 *    than or equal N, where N > 1.
 *
 * @author Mahmoud Parsian
 *
 */
public class WordCountDriver  extends Configured implements Tool {

    private static Logger THE_LOGGER = Logger.getLogger(WordCountDriver.class);

    /**
     *      Arguments are:
     *        args[0] = N (as integer)
     *        args[1] = input path
     *        args[2] = output path
     */   
    public int run(String[] args) throws Exception {

       Job job = new Job(getConf());
       job.setJarByClass(WordCountDriver.class);
       job.setJobName("WordCountDriver");
      
       // if a word length is leass than N,  
       // then that word will be ignored.
       int N = Integer.parseInt(args[0]);
       // brodcast N, which may be used in map() and reduce()
       job.getConfiguration().setInt("word.count.ignored.length", N);   
         
       job.setInputFormatClass(TextInputFormat.class); 
       job.setOutputFormatClass(TextOutputFormat.class);
      
       job.setOutputKeyClass(Text.class);         
       job.setOutputValueClass(IntWritable.class);    
          
       job.setMapperClass(WordCountMapper.class);
       // since addition is amonoid, we can have a combiner function
       job.setCombinerClass(WordCountCombiner.class);
       job.setReducerClass(WordCountReducer.class);
      
       // args[1] = input directory
       // args[2] = output directory
       FileInputFormat.setInputPaths(job, new Path(args[1]));
       FileOutputFormat.setOutputPath(job, new Path(args[2]));

       boolean status = job.waitForCompletion(true);
       THE_LOGGER.info("run(): status="+status);
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

       //int N = args[0];
       THE_LOGGER.info("N="+args[0]);

       //String inputDir = args[1];
       THE_LOGGER.info("inputDir="+args[1]);

       //String outputDir = args[2];
       THE_LOGGER.info("outputDir="+args[2]);

       int returnStatus = submitJob(args);
       THE_LOGGER.info("returnStatus="+returnStatus);
      
       System.exit(returnStatus);
    }


    /**
     * The main driver for word count map/reduce program.
     * Invoke this method to submit the map/reduce job.
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static int submitJob(String[] args) throws Exception {
       //     args[0] = N (as integer)
       //     args[1] = input path
       //     args[2] = output path
       int returnStatus = ToolRunner.run(new WordCountDriver(), args);
       return returnStatus;
    }
}

