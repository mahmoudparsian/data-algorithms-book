package org.dataalgorithms.chap24.mapreduce;

import java.io.File;
import java.util.Date;
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
import org.apache.hadoop.io.LongWritable;


/**

 * FastqCountBaseDriver
 *
 * @author Mahmoud Parsian
 *
 */
public class FastqCountBaseDriver  extends Configured implements Tool {

	private static Logger theLogger = Logger.getLogger(FastqCountBaseDriver.class);

	public int run(String [] args) throws Exception {

		theLogger.info("run(): input  args[0]="+args[0]);
		theLogger.info("run(): output args[1]="+args[1]);
		
		Job job = new Job(getConf());
		job.setJarByClass(FastqCountBaseDriver.class);
		job.setJobName("FastqCountBaseDriver");
		
		job.setInputFormatClass(FastqInputFormat.class); // cutom class		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// reducer will generate key as LongWritable
		job.setOutputKeyClass(Text.class);	
		// reducer will generate value as Text
		job.setOutputValueClass(LongWritable.class);		
		
		job.setMapperClass(FastqCountBaseMapper.class);
		job.setReducerClass(FastqCountBaseReducer.class);

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
			theLogger.info("ERROR: Wrong number of arguments: <input-dir> <output-dir>");
			throw new IllegalArgumentException("ERROR: Wrong number of arguments: <input-dir> <output-dir>");
		}

		String inputDir = args[0];
		theLogger.info("inputDir="+inputDir);

		String outputDir = args[1];
		theLogger.info("outputDir="+outputDir);

		int ret = ToolRunner.run(new FastqCountBaseDriver(), args);
		System.exit(ret);
	}


	/**
	* The main driver for word count map/reduce program.
	* Invoke this method to submit the map/reduce job.
	* @throws Exception When there is communication problems with the job tracker.
	*/
	public static int submitJob(String inputDir, String outputDir) throws Exception {
		String[] args = new String[2];
		args[0] = inputDir;
		theLogger.info("submitJob(): inputDir="+inputDir);
		args[1] = outputDir;
		theLogger.info("submitJob(): outputDir="+outputDir);
		int status = ToolRunner.run(new FastqCountBaseDriver(), args);
		theLogger.info("submitJob(): status="+status);
		return status;
	}
}
