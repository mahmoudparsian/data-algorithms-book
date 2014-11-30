package org.dataalgorithms.chap04.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;

/** 
 * LeftJoinUserMapper implements the map() function for 
 * the users part of "left join" design pattern.
 *
 * @author Mahmoud Parsian
 *
 */
public class LeftJoinUserMapper 
   extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

   PairOfStrings outputKey = new PairOfStrings();
   PairOfStrings outputValue = new PairOfStrings();

   /**
    * @param key is system generated, ignored
    * @param value is one uer record: <user_id><TAB><location_id>
    */
   @Override
   public void map(LongWritable key, Text value, Context context) 
      throws java.io.IOException, InterruptedException {
      String[] tokens = StringUtils.split(value.toString(), "\t");
      if (tokens.length == 2) {
      	 // tokens[0] = user_id
         // tokens[1] = location_id
         // to make sure location arrives before products
         outputKey.set(tokens[0], "1");    // set user_id
         outputValue.set("L", tokens[1]);  // set location_id
         context.write(outputKey, outputValue);
      }
   }

}
