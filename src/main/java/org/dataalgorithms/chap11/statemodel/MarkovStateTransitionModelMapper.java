package org.dataalgorithms.chap11.statemodel;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.commons.lang.StringUtils;

/**
 * The MarkovStateTransitionModelMapper class implements MapReduce's
 * map() method.
 *
 * 
 * @author Mahmoud Parsian
 *
 */
public class MarkovStateTransitionModelMapper 
	extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

	private PairOfStrings reducerKey = new PairOfStrings();
	private static final IntWritable ONE  = new IntWritable(1);

	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		// value = <customerID><,><State1><,><State2><,>...<,><StateN>
		String[] items = StringUtils.split(value.toString(), ",");	
		if (items.length > 2) {
			for (int i = 1; i < (items.length -1); i++) {
				reducerKey.set(items[i], items[i+1]);
				context.write(reducerKey, ONE);
			}
		}
	}              
}	
	
