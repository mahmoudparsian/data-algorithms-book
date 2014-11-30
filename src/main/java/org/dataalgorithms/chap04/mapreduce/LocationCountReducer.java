package org.dataalgorithms.chap04.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

/** 
 * LocationCountReducer implements the reduce() function for counting locations.
 *
 * @author Mahmoud Parsian
 *
 */
public  class LocationCountReducer 
	extends Reducer<Text, Text, Text, LongWritable> {

	Set<String> set = new HashSet<String>();
	
    @Override
    public void reduce(Text productID, Iterable<Text> locations, Context context)
        throws  IOException, InterruptedException {
        for (Text location: locations) {
           set.add(location.toString());
        }

        context.write(productID, new LongWritable(set.size()));
    }
}
