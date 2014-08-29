package org.dataalgorithms.chap11.statemodel;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * The MarkovStateTransitionModelCombiner class implements MapReduce's
 * combine() method (in Hadoop, we call it reduce() method).
 *
 * This class implements the combine() function for Markov's 
 * state transition model.
 * 
 * @author Mahmoud Parsian
 *
 */
public class MarkovStateTransitionModelCombiner 
	extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
		
	protected void reduce(PairOfStrings  key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		int partialSum = 0;
		for (IntWritable value : values) {
			partialSum += value.get();
		}
		context.write(key, new IntWritable(partialSum));       	
	}		
}	
	
