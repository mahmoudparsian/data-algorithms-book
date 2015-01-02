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
 * Produces  triads.
 *
 * @author Mahmoud Parsian
 *
 */ 
public class TriadsReducer 
   extends Reducer<PairOfLongs, LongWritable, Text, Text> {
   
    static final Text EMPTY = new Text("");
	
	// note that 0 is a fake node, which does not exist
	// key = PairOfLongs(<nodeA>, <nodeB>)
	// values = {0, n1, n2, n3, ...} 
	// OR
	// values = {n1, n2, n3, ...} 
	public void reduce(PairOfLongs key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {

		// list = {n1, n2, n3, ...} 
		List<Long> list = new ArrayList<Long>();
		boolean haveSeenSpecialNodeZero = false;
		for (LongWritable value : values) {
			long node = value.get();
			if (node == 0) {
				haveSeenSpecialNodeZero = true;
			}			
			else {
				list.add(node);
			}			
		}
		
		//debug:
		// System.out.println("TriadsReducer.reduce(): haveSeenSpecialNodeZero="+haveSeenSpecialNodeZero);
		// System.out.println("TriadsReducer.reduce(): key="+key.toString());
		// System.out.println("TriadsReducer.reduce(): list="+list.toString());

		if (haveSeenSpecialNodeZero) {
			if (list.isEmpty()) {
				// no triangles found
				return;
			}
			// emit triangles
			Text triangle = new Text();
			for (long node : list) {
				String triangleAsString = key.getLeftElement()  + 
				                          "," +
				                          key.getRightElement() + 
				                          "," +
				                          node;
				triangle.set(triangleAsString);
				context.write(triangle, EMPTY);
			}
		}
		else {
			// no triangles found
			return;
		}
	}
}
