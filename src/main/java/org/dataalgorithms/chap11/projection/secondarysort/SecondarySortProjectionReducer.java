package org.dataalgorithms.chap11.projection.secondarysort;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import edu.umd.cloud9.io.pair.PairOfLongInt;

import org.dataalgorithms.util.DateUtil;

/**
 * 
 * SecondarySortProjectionReducer 
 * 
 * Data arrive sorted to reducer.
 * 
 * MapReduce job for projecting customer transaction data
 * by using MapReduce's "secondary sort" (sort by shuffle 
 * function).
 * Note that reducer values arrive sorted by implementing 
 * the "secondary sort" design pattern (no data is sorted 
 * in memory).
 *
 * This class implements the reduce() function for "secondary sort" 
 * design pattern.
 * 
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortProjectionReducer extends MapReduceBase 
   implements Reducer<CompositeKey, PairOfLongInt, Text, Text> {
	 
	public void reduce(CompositeKey key, 
	                   Iterator<PairOfLongInt> values,
			           OutputCollector<Text, Text> output, 
			           Reporter reporter)
		throws IOException {

		// note that values are sorted (by using MR's secondary sort)
        // below, builder will generate: 
        //    CustoerID,Date1,Amount1,Date2,Amount2,...,DateN,AmountN
        // where Date1 <= Date2 <= ... <= DateN
        StringBuilder builder = new StringBuilder();
        builder.append(key.toString());			
        while (values.hasNext()) {
			 builder.append(",");
             PairOfLongInt pair = values.next();
			 long timestamp = pair.getLeftElement(); // date as milliseconds
			 String date = DateUtil.getDateAsString(timestamp);
			 builder.append(date); // date as String			 
			 builder.append(",");
			 builder.append(pair.getRightElement()); // amount
		} 
		
        output.collect(null, new Text(builder.toString()));        
	} // reduce

}
