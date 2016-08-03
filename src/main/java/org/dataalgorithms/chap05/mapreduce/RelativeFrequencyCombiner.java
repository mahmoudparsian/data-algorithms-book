package org.dataalgorithms.chap05.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/** 
 * RelativeFrequencyCombiner implements the combine() [in Hadoop, 
 * we use reduce() for implementing the combine() function] function 
 * for MapReduce/Hadoop.
 *
 * @author Mahmoud Parsian
 *
 */
public class RelativeFrequencyCombiner
        extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {

    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //
        int partialSum = 0;
        for (IntWritable value : values) {
            partialSum += value.get();
        }
        //
        context.write(key, new IntWritable(partialSum));
    }
}
