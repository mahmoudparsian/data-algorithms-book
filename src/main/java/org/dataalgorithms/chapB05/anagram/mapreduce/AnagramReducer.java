package org.dataalgorithms.chapB05.anagram.mapreduce;

import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
//
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
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //
        Set<String> set = new HashSet<>();
        for (Text value : values) {
            String word = value.toString();
            set.add(word);
        }
        //
        // if there are more than one word for a sorted word, 
        // then these are the anagrams
        //
        if (set.size() > 1) {
            context.write(key, new Text(set.toString()));
        }
    }
}
