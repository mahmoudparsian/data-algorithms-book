package org.dataalgorithms.chapB14.minmax.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * MinMaxReducer
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class MinMaxReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //
        String K = key.toString();
        //
        if (K.equals("min")) {
            int min = findMinimum(values);
            context.write(key, new IntWritable(min));
        }
        else if (K.equals("max")) {
            int max = findMaximum(values);
            context.write(key, new IntWritable(max));
        } 
        else {
        }

    }
    
    private static int findMinimum(Iterable<IntWritable> values) {
        int min = 0; // 0 is never used
        boolean firstTime = true;
        for (IntWritable v : values) {
            if (firstTime) {
                min = v.get();
                firstTime = false;
            } else {
                int value = v.get();
                if (value < min) {
                    min = value;
                }
            }
        }
        //
        return min;
    }
    
     private static int findMaximum(Iterable<IntWritable> values) {
        int max = 0; // 0 is never used
        boolean firstTime = true;
        for (IntWritable v : values) {
            if (firstTime) {
                max = v.get();
                firstTime = false;
            } else {
                int value = v.get();
                if (value > max) {
                    max = value;
                }
            }
        }
        //
        return max;
    }   
}
