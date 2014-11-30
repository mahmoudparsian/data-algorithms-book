package org.dataalgorithms.chap02.mapreduce;

import java.util.Date;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

import org.dataalgorithms.util.DateUtil;

/** 
 * SecondarySortMapper implements the map() function for 
 * the secondary sort design pattern.
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortMapper 
	extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {
 
   // reuse Hadoop's Writable objects
   private final CompositeKey reducerKey = new CompositeKey();
   private final NaturalValue reducerValue = new NaturalValue();
       
	/**
	 * @param key is Hadoop generated, not used here.
	 * @param value a record of <stockSymbol><,><Date><,><price>
	 */
	@Override
	public void map(LongWritable key, 
	                Text value,
			        Context context) 
	   throws IOException, InterruptedException {
			   
       String[] tokens = StringUtils.split(value.toString().trim(), ",");
       if (tokens.length == 3) {
          // tokens[0] = stokSymbol
          // tokens[1] = timestamp (as date)
          // tokens[2] = price as double
          Date date = DateUtil.getDate(tokens[1]);
          if (date == null) {
          	 return;
          }
          long timestamp = date.getTime();
          reducerKey.set(tokens[0], timestamp); 
          reducerValue.set(timestamp, Double.parseDouble(tokens[2]));
          // emit key-value pair
          context.write(reducerKey, reducerValue);
       }
       else {
          // ignore the entry or log as error, not enough tokens
       }
   }
}

