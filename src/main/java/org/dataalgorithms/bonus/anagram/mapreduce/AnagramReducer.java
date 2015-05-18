package org.dataalgorithms.bonus.anagram.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The Anagram reducer groups the values of the sorted keys that 
 * came in and checks to see if the values iterator contains more 
 * than one word. if the values contain more than one word we have 
 * spotted an anagram.
 *
 * @author Mahmoud Parsian
 *
 */

public class AnagramReducer 
    extends Reducer<Text, Text, Text, Text> {
   
    // This method is called once for each key. Most applications will 
    // define their reduce class by overriding this method. The default 
    // implementation is an identity function.
    public void reduce(Text key, Iterable<Text> values, Context context)
       throws IOException, InterruptedException {
       int numberOfAnagrams = 0;
       StringBuilder output = new StringBuilder();
       for (Text value : values) {
          String anagram = value.toString();
          output.append(anagram);
          output.append(",");
          numberOfAnagrams++;
       }
       if(numberOfAnagrams > 1) {
          context.write(key, new Text(output.toString()));
       }       
    }
}
