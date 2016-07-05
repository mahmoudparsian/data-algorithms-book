package org.dataalgorithms.chapB09.charcount.mapreduce.localaggregation;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

/**
 * Char Count Mapper
 *
 * For each line of input, break the line into words
 * and emit them as (<b>word</b>, <b>1</b>).
 *
 * @author Mahmoud Parsian
 *
 */
public class CharCountLocalAggregationMapper
    extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text reducerKey = new Text();
    private final LongWritable reducerValue = new LongWritable();
    

    // called once for each key/value pair in the input split. 
    // most applications should override this, but the default 
    // is the identity function.
    @Override
    public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
      
       String line = value.toString().trim();        
       if ((line == null) || (line.length() == 0)) {
           return;
       }
        
       String[] words = StringUtils.split(line);
       if ((words == null) || (words.length == 0)) {
           return;
       }
       
       Map<Character,Long> localmap = new HashMap<Character,Long>();
       for (String word : words) {
            char[] arr = word.toCharArray();
            for (char c : arr) {
                Long count = localmap.get(c);
                if (count == null) {
                    localmap.put(c, 1l);
                }
                else {
                    localmap.put(c, count+1);
                }
            }
        }
        
        // emit the localmap as k-v pairs
        for (Map.Entry<Character,Long> entry : localmap.entrySet()) {
            reducerKey.set(Character.toString(entry.getKey()));
            reducerValue.set(entry.getValue());
            context.write(reducerKey, reducerValue);
        }             
       
    }
     
}
