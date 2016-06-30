package org.dataalgorithms.chap28.mapreduce;

import java.io.IOException; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import edu.umd.cloud9.io.pair.PairOfLongInt;

/**
 * This is a mapper class for a monoidic MapReduce algorithm.
 *
 * PairOfLongInt = Tuple2<Long, Integer>
 * PairOfLongInt.getLeftElement() returns Long
 * PairOfLongInt.getRightElement() returns Integer
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class MeanMonoidizedMapper 
   extends Mapper<Text, LongWritable, Text, PairOfLongInt> {
 
   // PAIR_OF_SUM_AND_COUNT = (partial sum as long, count as int)
   private final static PairOfLongInt PAIR_OF_SUM_AND_COUNT = new PairOfLongInt();
 
   @Override
   public void map(Text key, LongWritable value, Context context)
       throws IOException, InterruptedException {
       // create a monoid
       PAIR_OF_SUM_AND_COUNT.set(value.get(), 1);
       context.write(key, PAIR_OF_SUM_AND_COUNT);
   }
}
 
