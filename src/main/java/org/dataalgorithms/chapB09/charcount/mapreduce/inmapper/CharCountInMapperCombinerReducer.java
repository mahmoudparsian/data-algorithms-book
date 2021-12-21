package org.dataalgorithms.chapB09.charcount.mapreduce.inmapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * A reducer class that just emits the sum of the input values.
 *
 * @author Mahmoud Parsian
 *
 */
public class CharCountInMapperCombinerReducer 
    extends Reducer<Text, LongWritable, Text, LongWritable> {
   
    // This method is called once for each key. Most applications will 
    // define their reduce class by overriding this method. The default 
    // implementation is an identity function.
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
       throws IOException, InterruptedException {
       long sum = 0;
       for (LongWritable count : values) {
           sum += count.get();
       }
       context.write(key, new LongWritable(sum));
    }
    
}


