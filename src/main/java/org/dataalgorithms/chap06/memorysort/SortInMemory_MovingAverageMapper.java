package org.dataalgorithms.chap06.memorysort;

import java.util.Date;
import java.io.IOException;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;
//
import org.dataalgorithms.util.DateUtil;
import org.dataalgorithms.chap06.TimeSeriesData;

/** 
 * SortInMemory_MovingAverageMapper implements the map() function for Moving Average.
 *
 * @author Mahmoud Parsian
 *
 */
public class SortInMemory_MovingAverageMapper    
    extends Mapper<LongWritable, Text, Text, TimeSeriesData> {
 
   // reuse Hadoop's Writable objects
   private final Text reducerKey = new Text();
   private final TimeSeriesData reducerValue = new TimeSeriesData();
 
   public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
       String record = value.toString();
       if ((record == null) || (record.length() == 0)) {
          return;
       }
       String[] tokens = StringUtils.split(record.trim(), ",");
       if (tokens.length == 3) {
          // tokens[0] = name of timeseries as string
          // tokens[1] = timestamp
          // tokens[2] = value of timeseries as double
          Date date = DateUtil.getDate(tokens[1]);
          if (date == null) {
          	 return;
          }
          reducerKey.set(tokens[0]); // set the name as key
          reducerValue.set(date.getTime(), Double.parseDouble(tokens[2]));
          context.write(reducerKey, reducerValue);
       }
       else {
          // log as error, not enough tokens
       }
   }
}
