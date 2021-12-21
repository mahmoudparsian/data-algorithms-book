package org.dataalgorithms.chapB09.charcount.mapreduce.inmapper;

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
public class CharCountInMapperCombinerMapper
    extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text reducerKey = new Text();
    private final LongWritable reducerValue = new LongWritable();
    
    private Map<Character,Long> map = null;

    // called once at the beginning of the task.   
    @Override
    protected void setup(Context context) throws IOException {
       map = new HashMap<Character,Long>();
    }

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
        
       for (String word : words) {
            char[] arr = word.toCharArray();
            for (char c : arr) {
                Long count = map.get(c);
                if (count == null) {
                    map.put(c, 1l);
                }
                else {
                    map.put(c, count+1);
                }
            }
        }
    }
    
   // called once at the end of the task.   
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Character,Long> entry : map.entrySet()) {
            reducerKey.set(Character.toString(entry.getKey()));
            reducerValue.set(entry.getValue());
            context.write(reducerKey, reducerValue);
        }                
    }    
   
}
