package org.dataalgorithms.chap01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** 
 * SecondarySortReducer implements the reduce() function for 
 * the secondary sort design pattern.
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortReducer 
    extends Reducer<DateTemperaturePair, Text, Text, Text> {

    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) 
    	throws IOException, InterruptedException {
    	StringBuilder builder = new StringBuilder();
    	for (Text value : values) {
    		builder.append(value.toString());
    		builder.append(",");
		}
        context.write(key.getYearMonth(), new Text(builder.toString()));
    }
}
