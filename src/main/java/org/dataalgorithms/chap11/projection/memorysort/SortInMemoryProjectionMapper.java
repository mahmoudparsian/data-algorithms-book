package org.dataalgorithms.chap11.projection.memorysort;

import java.util.Date;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;
import edu.umd.cloud9.io.pair.PairOfLongInt;

import org.dataalgorithms.util.DateUtil;


/**
 * This class, SortInMemoryProjectionMapper, implements the 
 * map() function for projecting customer transaction data
 * by using in memory sort (without secondary sort design pattern).
 *
 * @author Mahmoud Parsian
 *
 */
public class SortInMemoryProjectionMapper    
    extends Mapper<LongWritable, Text, Text, PairOfLongInt> {
 
   // reuse Hadoop's Writable objects
   private final Text reducerKey = new Text();
   // PairOfLongInt = pair(long, int) = pair(date, amount)
   private final PairOfLongInt reducerValue = new PairOfLongInt();
 
   public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
    
       String[] tokens = StringUtils.split(value.toString(), ",");
       if (tokens.length != 4) {
          // not a proper format
          return;
       }
       // tokens[0] = customer-id
       // tokens[1] = transaction-id
       // tokens[2] = purchase-date
       // tokens[3] = amount
       long date;
       try {
       		date = DateUtil.getDateAsMilliSeconds(tokens[2]);
       }
       catch(Exception e) {
           // not a proper date format
           return;
       }
       int amount = Integer.parseInt(tokens[3]);
       reducerValue.set(date, amount);
       reducerKey.set(tokens[0]);
       context.write(reducerKey, reducerValue);
   }
}
