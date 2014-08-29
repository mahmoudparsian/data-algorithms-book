package org.dataalgorithms.chap11.projection.secondarysort;

import java.util.Date;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.lang.StringUtils;
import edu.umd.cloud9.io.pair.PairOfLongInt;

import org.dataalgorithms.util.DateUtil;


/**
  * MapReduce job for projecting customer transaction data
  * by using MapReduce's "secondary sort" (sort by shuffle function).
  * Note that reducer values arrive sorted by implementing the "secondary sort"
  * design pattern (no data is sorted in memory).
  *
  * This class implements the map() function for "secondary sort" design pattern.
  * 
  * @author Mahmoud Parsian
  *
  */
public class SecondarySortProjectionMapper extends MapReduceBase 
   implements Mapper<LongWritable, Text, CompositeKey, PairOfLongInt> {
 
   // reuse Hadoop's Writable objects
   private final CompositeKey reducerKey = new CompositeKey();
   private final PairOfLongInt reducerValue = new PairOfLongInt();
 
	@Override
	public void map(LongWritable inkey, Text value,
			OutputCollector<CompositeKey, PairOfLongInt> output,
			Reporter reporter) throws IOException {
			   
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
       		// date is in error, ignore the record
       		return;
       }
       int amount = Integer.parseInt(tokens[3]);
       reducerValue.set(date, amount);
       reducerKey.set(tokens[0], date); 
       // emit key-value pair
       output.collect(reducerKey, reducerValue);
   }
}
