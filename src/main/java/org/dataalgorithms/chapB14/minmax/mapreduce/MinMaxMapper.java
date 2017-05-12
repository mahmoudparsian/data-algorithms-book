package org.dataalgorithms.chapB14.minmax.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * MinMaxMapper
 * 
 * This solution presented in Hadoop/MapReduce is an emulation 
 * of Apache Spark's mapPartitions() transformation.
 * 
 * Spark's mapPartitions() is expressed as a Hadoop/MapReduce mapper as:
 *    1. setup(): create necessary data structures
 * 
 *    2. map(): map input and use the data structures defined in the setup()
 * 
 *    3. cleanup(): emit required (key, value) pairs
 *
 * @author Mahmoud Parsian
 *
 */
public class MinMaxMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    
    private static final Text KEY_MIN = new Text("min");
    private static final Text KEY_MAX = new Text("max");

    private int min;
    private int max;

    // Called once at the beginning of the task.  
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        //
        min = Integer.MAX_VALUE;
        max = Integer.MIN_VALUE;
    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        //
        String valueAsString = value.toString();
        int valueAsInteger = Integer.parseInt(valueAsString);
        //
        if (valueAsInteger > max) {
            max = valueAsInteger;
        }
        //           
        if (valueAsInteger < min) {
            min = valueAsInteger;
        }
    }
    
    // Called once at the end of the task.    
    @Override
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {
        //
        context.write(KEY_MIN, new IntWritable(min));
        context.write(KEY_MAX, new IntWritable(max));
    }    
}
