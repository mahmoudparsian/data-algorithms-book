package org.dataalgorithms.chapB14.minmax.mapreduce;

import org.apache.log4j.Logger;
//
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 *
 * MinMaxDriver: find minimum and maximum of a given set of numbers
 * 
 * This solution presented in Hadoop/MapReduce is an emulation 
 * of Apache Spark's mapPartitions() transformation.
 * 
 * Spark's mapPartitions() is expressed as a Hadoop/MapReduce mapper as:
 *    1. setup(): create necessary data structures (define min and max)
 * 
 *    2. map(): map input and use the data structures defined in the setup()
 *      note that no emit(key, value) is done in the map()
 * 
 *    3. cleanup(): emit required (key, value) pairs
 *
 * @author Mahmoud Parsian
 *
 */
public class MinMaxDriver extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(MinMaxDriver.class);

    @Override
    public int run(String[] args) throws Exception {

        LOGGER.info("run(): input  args[0]=" + args[0]);
        LOGGER.info("run(): output args[1]=" + args[1]);
        //
        Job job = new Job(getConf());
        job.setJarByClass(MinMaxDriver.class);
        job.setJobName("MinMaxDriver");
        //
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // reducer will generate key as Text
        job.setOutputKeyClass(Text.class);
        // reducer will generate value as Integer
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(MinMaxMapper.class);
        job.setReducerClass(MinMaxReducer.class);
        //
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //
        boolean status = job.waitForCompletion(true);
        LOGGER.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 2) {
            LOGGER.info("ERROR: Wrong number of arguments: <input-dir> <output-dir>");
            throw new IllegalArgumentException("ERROR: Wrong number of arguments: <input-dir> <output-dir>");
        }
        //
        String inputDir = args[0];
        LOGGER.info("inputDir=" + inputDir);
        //
        String outputDir = args[1];
        LOGGER.info("outputDir=" + outputDir);
        //
        int ret = ToolRunner.run(new MinMaxDriver(), args);
        System.exit(ret);
    }

    public static int submitJob(String inputDir, String outputDir) throws Exception {
        String[] args = new String[2]; // define an array of length 2
        //
        args[0] = inputDir;
        LOGGER.info("submitJob(): inputDir=" + inputDir);
        //
        args[1] = outputDir;
        LOGGER.info("submitJob(): outputDir=" + outputDir);
        //
        int status = ToolRunner.run(new MinMaxDriver(), args);
        LOGGER.info("submitJob(): status=" + status);
        return status;
    }
}
