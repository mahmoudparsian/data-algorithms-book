package org.dataalgorithms.chap08.mapreduce;

import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;

import org.dataalgorithms.util.HadoopUtil;

/**
 * CommonFriendsDriverUsingList
 *
 * @author Mahmoud Parsian
 *
 */
public class CommonFriendsDriverUsingList  extends Configured implements Tool {

    private static Logger theLogger = Logger.getLogger(CommonFriendsDriverUsingList.class);

    public int run(String[] args) throws Exception {
            
        Job job = new Job(getConf());
        job.setJobName("CommonFriendsDriverUsingList");

        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache(job, "/lib/");
        
        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // mapper will generate key as Text (the keys are as (person1,person2))
        job.setOutputKeyClass(Text.class);
        
        // mapper will generate value as ArrayListOfLongsWritable (list of friends)        
        job.setOutputValueClass(ArrayListOfLongsWritable.class);     
            
        job.setMapperClass(CommonFriendsMapperUsingList.class);
        job.setReducerClass(CommonFriendsReducerUsingList.class);

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
            throw new IllegalArgumentException("usage: Argument 1: input dir, Argument 2: output dir");
        }

        theLogger.info("inputDir="+args[0]);
        theLogger.info("outputDir="+args[1]);
        int jobStatus = submitJob(args);
        theLogger.info("jobStatus="+jobStatus);    
        System.exit(jobStatus);
    }


    /**
    * The main driver for word count map/reduce program.
    * Invoke this method to submit the map/reduce job.
    * @throws Exception When there is communication problems with the job tracker.
    */
    public static int submitJob(String[] args) throws Exception {
        int jobStatus = ToolRunner.run(new CommonFriendsDriverUsingList(), args);
        return jobStatus;
    }
}

