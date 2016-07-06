package org.dataalgorithms.chapB01.wordcount.mapreduce;

import java.io.IOException;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//
import org.apache.commons.lang.StringUtils;

/**
 * Word Count Mapper
 *
 * For each line of input, break the line into words
 * and emit them as (<b>word</b>, <b>1</b>).
 *
 * @author Mahmoud Parsian
 *
 */
public class WordCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int DEFAULT_IGNORED_LENGTH = 3; // default
    private int N = DEFAULT_IGNORED_LENGTH; 
    private static final IntWritable ONE = new IntWritable(1);
    private final Text reducerKey = new Text();

    // called once at the beginning of the task.   
    @Override
    protected void setup(Context context)
       throws IOException,InterruptedException {
       this.N = context.getConfiguration().getInt("word.count.ignored.length", 
                                                  DEFAULT_IGNORED_LENGTH);
    }

    // called once for each key/value pair in the input split. 
    // most applications should override this, but the default 
    // is the identity function.
    @Override
    public void map(LongWritable key, Text value, Context context)
       throws IOException, InterruptedException {
      
       String line = value.toString().trim();        
       if ((line == null) || (line.length() < this.N)) {
           return;
       }
        
       String[] words = StringUtils.split(line);
       if (words == null) {
           return;
       }
        
       for (String word : words) {
            if (word.length() < this.N) {
               // ignore strings with less than size N
               continue;
            }
            if (word.matches(".*[,.;]$")) {
                // remove the special char from the end
                word = word.substring(0, word.length() -1); 
            }
            reducerKey.set(word);
            context.write(reducerKey, ONE);
       }
    }
   
}
