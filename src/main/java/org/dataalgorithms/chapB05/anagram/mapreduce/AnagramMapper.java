package org.dataalgorithms.chapB05.anagram.mapreduce;

import java.io.IOException;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//
import org.apache.commons.lang.StringUtils;
//
import java.util.Arrays;

/**
 * This mapper class reads a record of input (comprised of words). 
 * For each word, sorts the letters in the word and
 * writes its back to the Hadoop's output collector as
 *
 *    Key : sorted word (letters in the word sorted) 
 *    Value: the word itself as the value.
 *
 * When the reducer runs then we can group anagrams together based on the sorted key.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class AnagramMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    // reuse the place holders for output key/value
    private final Text keyAsSortedText = new Text();
    private final Text valueAsOrginalText = new Text();

    private static final int DEFAULT_IGNORED_LENGTH = 3; // default
    private int N = DEFAULT_IGNORED_LENGTH;

    // called once at the beginning of the task.   
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("word.count.ignored.length", DEFAULT_IGNORED_LENGTH);
    }

    // called once for each key/value pair in the input split. 
    // most applications should override this, but the default 
    // is the identity function.
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //
        if (value == null) {
            return;
        }
        //
        String valueAsString = value.toString();
        if (valueAsString == null) {
            return;
        }
        //       
        String line = valueAsString.trim().toLowerCase();
        if ((line == null) || (line.length() < this.N)) {
            return;
        }
        // 
        String[] words = StringUtils.split(line);
        if (words == null) {
            return;
        }
        // 
        for (String word : words) {
            if (word.length() < this.N) {
                // ignore strings with less than size N
                continue;
            }
            if (word.matches(".*[,.;]$")) {
                // remove the special char from the end
                word = word.substring(0, word.length() - 1);
            }
            if (word.length() < this.N) {
                // ignore strings with less than size N
                continue;
            }
            //
            String sortedWord = sort(word);
            keyAsSortedText.set(sortedWord);
            valueAsOrginalText.set(word);
            context.write(keyAsSortedText, valueAsOrginalText);
        }
    }
    
    static String sort(final String word) {
        char[] chars = word.toCharArray();
        Arrays.sort(chars);
        String sortedWord = String.valueOf(chars);
        return sortedWord;
    }        
}
