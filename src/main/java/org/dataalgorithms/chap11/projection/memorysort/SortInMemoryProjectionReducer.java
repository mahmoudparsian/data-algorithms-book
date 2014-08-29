package org.dataalgorithms.chap11.projection.memorysort;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import edu.umd.cloud9.io.pair.PairOfLongInt;

import org.dataalgorithms.util.DateUtil;

/**
 *
 * This class, SortInMemoryProjectionReducer, implements the 
 * reduce() function for projecting customer transaction data
 * by using in memory sort (without secondary sort design pattern).
 *
 * 
 * Note that data arrive to reducer, which are NOT sorted.
 * We have to do in memory sort before emitting final output.
 * 
 * @author Mahmoud Parsian
 */
public class SortInMemoryProjectionReducer 
   extends Reducer<Text, PairOfLongInt, Text, Text> {

	public void reduce(Text key, Iterable<PairOfLongInt> values, Context context)	
		throws IOException, InterruptedException {

		// build the unsorted list of timeseries
		List<PairOfLongInt> list = new ArrayList<PairOfLongInt>();
		for (PairOfLongInt pair : values) {
			PairOfLongInt copy = pair.clone();
			list.add(copy);
		} 
		
		// sort the projected data
        Collections.sort(list);
        
        // builder will generate: 
        //   CustomerID,Date1,Amount1,Date2,Amount2,...,DateN,AmountN
        //      where Dat1 <= Date2 <= ... <= DateN
        StringBuilder builder = new StringBuilder();
        builder.append(key.toString());			
		for (PairOfLongInt pair : list) {
        	builder.append(",");
			long timestamp = pair.getLeftElement(); // date as milliseconds
			String date = DateUtil.getDateAsString(timestamp);
			builder.append(date); // date
			builder.append(",");
			builder.append(pair.getRightElement()); // amount
		} 
		
        // context.write(key, new Text(builder.toString()));
        context.write(null, new Text(builder.toString()));
	} // reduce

}
