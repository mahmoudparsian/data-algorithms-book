package org.dataalgorithms.chap11.statemodel;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.pair.PairOfStrings;

import org.dataalgorithms.util.HadoopUtil;

/**
 * Markov state transition probability matrix Driver
 *
 * 
 * @author Mahmoud Parsian
 *
 */
public class MarkovStateTransitionModelDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJobName("MarkovStateTransitionModelDriver");
        
        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache(job, "/lib/");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MarkovStateTransitionModelMapper.class);
        job.setReducerClass(MarkovStateTransitionModelReducer.class);
        job.setCombinerClass(MarkovStateTransitionModelCombiner.class);
        
        // PairOfStrings = (fromState, toState)
        job.setMapOutputKeyClass(PairOfStrings.class); 
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static void main(String[] args) throws Exception {
        int statusCode = ToolRunner.run(new MarkovStateTransitionModelDriver(), args);
        System.exit(statusCode);
	}
}
