package org.dataalgorithms.chap16.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Produces unique triangles
 *
 * @author Mahmoud Parsian
 *
 */ 
public class UniqueTriadsReducer 
   extends Reducer<Text, Text, Text, Text> {
   
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		// values = set of empty strings (ignored)
		// Produce unique triads 
		context.write(key, null);
	}
}
