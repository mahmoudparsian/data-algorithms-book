package org.dataalgorithms.chapB12.docwordcount.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * A reducer class that just emits the sum of the input values.
 *
 * @author Mahmoud Parsian
 *
 */
public class WordCountReducer 
    extends Reducer<Text, IntWritable, Text, IntWritable> {
   
    // This method is called once for each key. Most applications will 
    // define their reduce class by overriding this method. The default 
    // implementation is an identity function.
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
       throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable count : values) {
           sum += count.get();
       }
       context.write(key, new IntWritable(sum));
    }
    
}


