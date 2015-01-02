package org.dataalgorithms.chap16.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import edu.umd.cloud9.io.pair.PairOfLongs;

/**
 * Produces original edges and triads.
 *
 * @author Mahmoud Parsian
 *
 */ 
public class GraphEdgeReducer 
   extends Reducer<LongWritable, LongWritable, PairOfLongs, LongWritable> {
    
    // reuse objects
	PairOfLongs k2 = new PairOfLongs();
	LongWritable v2 = new LongWritable();

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {

		List<Long> list = new ArrayList<Long>();
		// we assume that no node has an ID of zero
		// ID of zero (0) is a special node, which does not exist
		v2.set(0);
		for (LongWritable value : values) {
			list.add(value.get());
			k2.set(key.get(), value.get());
			context.write(k2, v2);
		}

		// Sort and Generate triads.
		Collections.sort(list);
		v2.set(key.get());
		for (int i=0; i< list.size() -1; ++i) {
			for (int j=i+1; j< list.size(); ++j) {
				k2.set(list.get(i), list.get(j));
				context.write(k2, v2);
			}
		}
	}
}
