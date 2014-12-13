package org.dataalgorithms.chap24.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * FastaCountBaseDriver class is the "driver" class. 
 * it sets everything up, then gets it started.
 * 
 * @author Mahmoud Parsian
 *
 */
public class FastaCountBaseDriver  extends Configured implements Tool {

 	private static final Logger theLogger = Logger.getLogger(FastaCountBaseDriver.class);

	public int run(String[] args) throws Exception {	
		Job job = new Job(getConf(), "count-dns-bases");
		job.setJarByClass(FastaCountBaseDriver.class);	
        job.setMapperClass(FastaCountBaseMapper.class);
        job.setReducerClass(FastaCountBaseReducer.class);
        job.setNumReduceTasks(5); // worked
        //job.setCombinerClass(FastaCountBaseCombiner.class);
	    job.setInputFormatClass(FastaInputFormat.class);
	    job.setPartitionerClass(BasePartitioner.class); // worked
	    job.setSortComparatorClass(BaseComparator.class); // worked
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
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
			theLogger.warn("ERROR: Wrong number of arguments. <input-dir> <output-dir>");
			throw new IllegalArgumentException("ERROR: Wrong number of arguments. <input-dir> <output-dir>");
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
		//String[] args = new String[2];
		//args[0] = inputDir;
		//theLogger.info("submitJob(): inputDir="+inputDir);
		//args[1] = outputDir;
		//theLogger.info("submitJob(): outputDir="+outputDir);
		int returnStatus = ToolRunner.run(new FastaCountBaseDriver(), args);
		return returnStatus;
	}
}

