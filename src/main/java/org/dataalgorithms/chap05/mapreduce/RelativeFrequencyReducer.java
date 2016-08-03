package org.dataalgorithms.chap05.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * RelativeFrequencyReducer implements the reduce() function for Relative Frequency of words.
 *
 * @author Mahmoud Parsian
 *
 */
public class RelativeFrequencyReducer
        extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {

    private double totalCount = 0;
    private final DoubleWritable relativeCount = new DoubleWritable();
    private String currentWord = "NOT_DEFINED";

    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        if (key.getNeighbor().equals("*")) {
            if (key.getWord().equals(currentWord)) {
                totalCount += totalCount + getTotalCount(values);
            } else {
                currentWord = key.getWord();
                totalCount = getTotalCount(values);
            }
        } else {
            int count = getTotalCount(values);
            relativeCount.set((double) count / totalCount);
            context.write(key, relativeCount);
        }

    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}
