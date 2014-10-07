package org.dataalgorithms.chap29.combinesmallfilesbybuckets;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import org.dataalgorithms.util.HadoopUtil;

/**
 * WordCountDriverWithConsolidator
 * The driver class for a word count
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class WordCountDriverWithConsolidator 
    extends Configured implements Tool {

	private static final Logger THE_LOGGER = Logger.getLogger(WordCountDriverWithConsolidator.class);
	private static int NUMBER_OF_MAP_SLOTS_AVAILABLE = 8;
	private static int MAX_FILES_PER_BUCKET	= 5;
	
	// instance variables
	private String inputDir = null;
	private String outputDir = null;
	private Job job = null;
		
	public WordCountDriverWithConsolidator(String inputDir, String outputDir) {
		this.inputDir = inputDir;
		this.outputDir = outputDir;
	}

	public Job getJob() {
		return this.job;
	}

	/**
	 * runs this tool.
	 */
	public int run(String[] args) throws Exception {		
		this.job = new Job(getConf(), "WordCountDriverWithConsolidator");
		job.setJobName("WordCountDriverWithConsolidator");
		job.getConfiguration().setInt("word.count.ignored.length", 3);	
           
		// add jars to DistributedCache	(this way, there is no need to re-start hadoop cluster)
		HadoopUtil.addJarsToDistributedCache(job, "/lib/");

		// prepare input
		FileSystem fs = FileSystem.get(job.getConfiguration());
        List<String> smallFiles = HadoopUtil.listDirectoryAsListOfString(inputDir, fs);
		int size = smallFiles.size();
		if (size <= NUMBER_OF_MAP_SLOTS_AVAILABLE) {
			// we have 45 slots for map(); we have enough mappers per bioset
			for (String file : smallFiles) {
				THE_LOGGER.info("file=" + file);
				addInputPath(fs, job, file);
			}
		}
		else {
			//
			// here size > NUMBER_OF_MAP_SLOTS_AVAILABLE
			//  parentDir will  be "/tmp/<uuid>/" (has a trailing "/")
			//  targetDir will  be "/tmp/<uuid>/id/"
			//  targetFile will be "/tmp/<uuid>/id/id"
			// create and fill buckets... each bucket will many small files  
			BucketThread[] buckets = SmallFilesConsolidator.createBuckets(size, NUMBER_OF_MAP_SLOTS_AVAILABLE, MAX_FILES_PER_BUCKET);	
			SmallFilesConsolidator.fillBuckets(buckets, smallFiles, job, MAX_FILES_PER_BUCKET);
			SmallFilesConsolidator.mergeEachBucket(buckets, job);
		}
		
    	// prepare output 
		FileOutputFormat.setOutputPath(job, new Path(outputDir));	
							
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);			
		job.setOutputValueClass(IntWritable.class);	 
    		
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		boolean status = job.waitForCompletion(true);
		THE_LOGGER.info("run(): status="+status);
		return status ? 0 : 1;
	}
	
	private void addInputPath(FileSystem fs, Job job, String pathAsString) {
		try {
			Path path = new Path(pathAsString);
			if (HadoopUtil.pathExists(path, fs)) {
				FileInputFormat.addInputPath(job, path);
			}
			else {
				THE_LOGGER.info("addInputPath(): path does not exist. ignored: "+pathAsString);	
			}
		}
		catch(Exception e) {
			// some data for biosets might not exist 
			THE_LOGGER.error("addInputPath(): could not add path: "+ pathAsString, e);
		}
	}

	/**
	 * The main driver for word count map/reduce program.
	 * Invoke this method to submit the map/reduce job.
	 * @param requestID indicates where the results will be posted 
	 * @param biosetIds list of biosets Ids to be processed
	 * @throws Exception failed to submit/complete the job; when there is communication problems with the job tracker.
	 */
	public static int submitJob(String inputDir, String outputDir)
		throws Exception {		
		// submit job
		WordCountDriverWithConsolidator driver = new WordCountDriverWithConsolidator(inputDir, outputDir);
		int status = ToolRunner.run(driver, null);
		THE_LOGGER.info("submitJob(): status="+status);
		return status;
	}
	
	/**
	* The main driver for word count map/reduce program.
	* Invoke this method to submit the map/reduce job.
	* @throws Exception When there is communication problems with the job tracker.
	*/
	public static void main(String[] args) throws Exception {
		// Make sure there are exactly 2 parameters
		if (args.length != 2) {
			THE_LOGGER.warn("2 arguments. <input-dir>, <output-dir>");
			throw new IllegalArgumentException("2 arguments. <input-dir>, <output-dir>");
		}

		THE_LOGGER.info("inputDir="+args[0]);
		THE_LOGGER.info("outputDir="+args[1]);
		long startTime = System.currentTimeMillis();	
		int returnStatus = submitJob(args[0], args[1]);
		long elapsedTime = System.currentTimeMillis() - startTime;
		THE_LOGGER.info("returnStatus="+returnStatus);		
		THE_LOGGER.info("Finished in milliseconds: " + elapsedTime );				
		System.exit(returnStatus);
	}
}
