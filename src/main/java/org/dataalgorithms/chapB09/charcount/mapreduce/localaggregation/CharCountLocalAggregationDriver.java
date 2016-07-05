package org.dataalgorithms.chapB09.charcount.mapreduce.localaggregation;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * Description:

    CharCountWithPairsDriver: submits the job to Hadoop Counting the chars.
 *
 * @author Mahmoud Parsian
 *
 */
public class CharCountLocalAggregationDriver  extends Configured implements Tool {

    private static Logger theLogger = Logger.getLogger(CharCountLocalAggregationDriver.class);

    /**
     *      Arguments are:
     *        args[0] = input path
     *        args[1] = output path
     */   
    @Override
    public int run(String[] args) throws Exception {

       Job job = new Job(getConf());
       job.setJarByClass(CharCountLocalAggregationDriver.class);
       job.setJobName("CharCountWithLocalAggregationDriver");
         
       job.setInputFormatClass(TextInputFormat.class); 
       job.setOutputFormatClass(TextOutputFormat.class);
      
       job.setOutputKeyClass(Text.class);         
       job.setOutputValueClass(LongWritable.class);    
          
       job.setMapperClass(CharCountLocalAggregationMapper.class);
       // since addition is amonoid, we can have a combiner function
       job.setCombinerClass(CharCountLocalAggregationReducer.class);
       job.setReducerClass(CharCountLocalAggregationReducer.class);
      
       // args[0] = input directory
       // args[1] = output directory
       FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
       // Make sure there are exactly 2 parameters
       if (args.length != 2) {
          throw new IllegalArgumentException("usage: <input> <output>");
       }

       //String inputDir = args[0];
       theLogger.info("inputDir="+args[0]);
       //String outputDir = args[1];
       theLogger.info("outputDir="+args[1]);
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
       int returnStatus = ToolRunner.run(new CharCountLocalAggregationDriver(), args);
       return returnStatus;
    }
}

