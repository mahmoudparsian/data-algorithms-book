package org.dataalgorithms.chap07.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Implement the reduce() function.
 * MBAReducer aggregates count of all items paired.
 *
 * Market Basket Analysis Algorithm: find the association rule for the list of items 
 * in a basket; That is, there are transaction data in a store
 * <ul>
 * <li>trx1: apple, cracker, soda, corn </li>
 * <l1>trax2: icecream, soda, bread</li>
 * <li>...</li>
 * <ul>
 * 
 * <p>
 * The code reads the data as 
 * key: first item
 * value: the rest of the items
 *
 * @author Mahmoud Parsian
 *
 */
public class MBAReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
         
   @Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
      int sum = 0; // total items paired
      for (IntWritable value : values) {
         sum += value.get();
      }
      context.write(key, new IntWritable(sum));
   }
}

