package org.dataalgorithms.chap28.mapreduce;

import java.io.IOException; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import edu.umd.cloud9.io.pair.PairOfLongInt;

/**
 * This is a mapper class for a monodic MapReduce algorithm.
 *
 * PairOfLongInt = Tuple2<Long, Integer>
 * PairOfLongInt.getLeftElement() returns Long
 * PairOfLongInt.getRightElement() returns Integer
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class MeanMonodizedMapper 
   extends Mapper<Text, LongWritable, Text, PairOfLongInt> {
 
   // pairOfSumAndCount = (partial sum as long, count as int)
   private final static PairOfLongInt pairOfSumAndCount = new PairOfLongInt();
 
   public void map(Text key, LongWritable value, Context context)
       throws IOException, InterruptedException {
       // create a monoid
       pairOfSumAndCount.set(value.get(), 1);
       context.write(key, pairOfSumAndCount);
   }
}
 
