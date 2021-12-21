package org.dataalgorithms.chap17.mapreduce;

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
import org.apache.hadoop.io.IntWritable;

/**
 * KmerCountDriver
 *
 * @author Mahmoud Parsian
 *
 */
public class KmerCountDriver extends Configured implements Tool {

    private static Logger theLogger = Logger.getLogger(KmerCountDriver.class);

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(KmerCountDriver.class);
        job.setJobName("KmerCountDriver");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);		// reducer key as Text 
        job.setOutputValueClass(IntWritable.class);	// reducer value as int    

        job.setMapperClass(KmerCountMapper.class);
        job.setReducerClass(KmerCountReducer.class);

        Path inputDirectory = new Path(args[0]);
        Path outputDirectory = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputDirectory);
        FileOutputFormat.setOutputPath(job, outputDirectory);

        // set k-mer size
        job.getConfiguration().setInt("k.mer", Integer.parseInt(args[2]));

        boolean status = job.waitForCompletion(true);
        theLogger.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    /**
     * The main driver for word count map/reduce program. 
     * Invoke this method to submit the map/reduce job.
     *
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 3) {
            throw new IllegalArgumentException("arguments: <input> <output> <Kmer>");
        }
        theLogger.info("inputDir=" + args[0]);
        theLogger.info("outputDir=" + args[1]);
        theLogger.info("kmer=" + args[2]);
        int returnStatus = submitJob(args);
        theLogger.info("returnStatus=" + returnStatus);
        System.exit(returnStatus);
    }

    /**
     * The main driver for word count map/reduce program. 
     * Invoke this method to submit the map/reduce job.
     *
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static int submitJob(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new KmerCountDriver(), args);
        return returnStatus;
    }
}
