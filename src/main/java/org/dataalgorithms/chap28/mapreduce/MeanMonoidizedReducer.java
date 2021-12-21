package org.dataalgorithms.chap28.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import edu.umd.cloud9.io.pair.PairOfLongInt;
 
/**
 * This is a mapper class for a monodic MapReduce algorithm.
 *
 * PairOfLongInt = Tuple2<Long, Integer>
 * PairOfLongInt.getLeftElement() returns Long
 * PairOfLongInt.getRightElement() returns Integer
 *
 * @author Mahmoud Parsian
 *
 */
public class MeanMonoidizedReducer
   extends Reducer<Text, PairOfLongInt, Text, DoubleWritable> {

   public void reduce(Text key, Iterable<PairOfLongInt> values, Context context)
      throws IOException, InterruptedException {
      long sum = 0;
      int count = 0;
      for (PairOfLongInt pair : values) {
         sum += pair.getLeftElement(); 
         count += pair.getRightElement(); 
      }
      context.write(key, new DoubleWritable(sum / count));
   }
}
