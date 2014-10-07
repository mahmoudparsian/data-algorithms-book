package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
  
/**
 *  A reducer class that just emits the sum of the input values.
 *
 * @author Mahmoud Parsian
 *
 */
public class WordCountReducer extends 
	 Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, 
                       Iterable<IntWritable> values,
                       Context context) 
       throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable val : values) {
          sum += val.get();
       }
       context.write(key, new IntWritable(sum)); 
    }
}
