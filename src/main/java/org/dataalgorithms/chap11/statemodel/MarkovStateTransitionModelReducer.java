package org.dataalgorithms.chap11.statemodel;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * The MarkovStateTransitionModelReducer class implements MapReduce's
 * reduce() method.
 *
 * 
 * @author Mahmoud Parsian
 *
 */
public class MarkovStateTransitionModelReducer 
	extends Reducer<PairOfStrings, IntWritable, Text, IntWritable> {
	   	
	protected void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
	throws IOException, InterruptedException {
		int finalCount = 0;
		for (IntWritable value : values) {
			finalCount += value.get();
		}
		
		String fromState = key.getLeftElement();
		String toState = key.getRightElement();
		String outputkey = fromState + "," + toState;
		context.write(new Text(outputkey), new IntWritable(finalCount));
	}	   	
}
	

