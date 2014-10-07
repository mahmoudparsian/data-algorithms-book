package org.dataalgorithms.chap29.combinesmallfilesbybuckets;

import org.apache.log4j.Logger;
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

import org.dataalgorithms.util.HadoopUtil;

/**
 * WordCountDriverWithoutConsolidator
 * The driver class for a word count
 *
 * @author Mahmoud Parsian
 *
 */
public class WordCountDriverWithoutConsolidator  
    extends Configured implements Tool {

	private static Logger theLogger = Logger.getLogger(WordCountDriverWithoutConsolidator.class);

	public int run(String[] args) throws Exception {
    		
		Job job = new Job(getConf());
		// add jars to DistributedCache	(this way, there is no need to re-start hadoop cluster)
		HadoopUtil.addJarsToDistributedCache(job, "/lib/");

		job.setJobName("WordCountDriverWithoutConsolidator");
		job.getConfiguration().setInt("word.count.ignored.length", 3);	
			
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(IntWritable.class);	 
    		
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
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
			theLogger.warn("2 arguments. <input-dir>, <output-dir>");
			throw new IllegalArgumentException("2 arguments. <input-dir>, <output-dir>");
		}

		theLogger.info("inputDir="+args[0]);
		theLogger.info("outputDir="+args[1]);
		long startTime = System.currentTimeMillis();	
		int returnStatus = submitJob(args);
		long elapsedTime = System.currentTimeMillis() - startTime;
		theLogger.info("returnStatus="+returnStatus);		
		theLogger.info("Finished in milliseconds: " + elapsedTime );					
		System.exit(returnStatus);
	}


	/**
	* The main driver for word count map/reduce program.
	* Invoke this method to submit the map/reduce job.
	* @throws Exception When there is communication problems with the job tracker.
	*/
	public static int submitJob(String[] args) throws Exception {
		//args[0] = inputDir;
		//args[1] = outputDir;
		int returnStatus = ToolRunner.run(new WordCountDriverWithoutConsolidator(), args);
		return returnStatus;
	}
}

