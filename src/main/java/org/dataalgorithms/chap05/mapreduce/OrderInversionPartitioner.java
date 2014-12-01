package org.dataalgorithms.chap05.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/** 
 * This is an plug-in class.
 * The OrderInversionPartitioner class indicates how to partition data
 * based on the word only (the natural key).
 * 
 * @author Mahmoud Parsian
 *
 */
public class OrderInversionPartitioner 
   extends Partitioner<PairOfWords, IntWritable> {

    @Override
    public int getPartition(PairOfWords key, 
                            IntWritable value, 
                            int numberOfPartitions) {
        // key = (leftWord, rightWord) = (word, neighbor)
        String leftWord = key.getLeftElement();
        return Math.abs( ((int) hash(leftWord)) % numberOfPartitions);
    }
    
    /**
     * Return a hashCode() of a given String object.   
     * This is adapted from String.hashCode()
     *
     * @param str a string object
     *
     */
    private static long hash(String str) {
       long h = 1125899906842597L; // prime
       int length = str.length();
       for (int i = 0; i < length; i++) {
          h = 31*h + str.charAt(i);
       }
       return h;
    }	
    
}
