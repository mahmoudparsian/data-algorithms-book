package org.dataalgorithms.chap28.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import edu.umd.cloud9.io.pair.PairOfLongInt;

/**
 * This is a combiner class for a monodic MapReduce algorithm.
 *
 * PairOfLongInt = Tuple2<Long, Integer>
 * PairOfLongInt.getLeftElement() returns Long
 * PairOfLongInt.getRightElement() returns Integer
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class MeanMonoidizedCombiner
   extends Reducer<Text,PairOfLongInt,Text,PairOfLongInt> {
 
   @Override
   public void reduce(Text key, Iterable<PairOfLongInt> values, Context context)
      throws IOException, InterruptedException {
      long partialSum = 0;
      int partialCount = 0;
      for (PairOfLongInt pair : values) {
         partialSum += pair.getLeftElement(); // partial sum as long
         partialCount += pair.getRightElement(); // partial count as int
      }
      context.write(key, new PairOfLongInt(partialSum, partialCount));
   }
}
 
