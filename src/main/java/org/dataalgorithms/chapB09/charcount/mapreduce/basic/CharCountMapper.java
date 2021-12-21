package org.dataalgorithms.chapB09.charcount.mapreduce.basic;

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
public class CharCountMapper
    extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text reducerKey = new Text();
    private static final LongWritable ONE = new LongWritable(1);

    // called once for each key/value pair in the input split. 
    // most applications should override this, but the default 
    // is the identity function.
    @Override
    public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
      
       String line = value.toString().toLowerCase().trim();        
       if ((line == null) || (line.length() == 0)) {
           return;
       }
        
       String[] words = StringUtils.split(line);
       if ((words == null) || (words.length == 0)) {
           return;
       }
        
       for (String word : words) {
            char[] arr = word.toCharArray();
            for (char c : arr) {
                reducerKey.set(Character.toString(c));
                context.write(reducerKey, ONE);

            }
        }
    }
   
}
