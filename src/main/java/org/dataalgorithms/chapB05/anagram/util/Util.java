package org.dataalgorithms.chapB05.anagram.util;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
//
import scala.Tuple2;
//
import org.apache.commons.lang.StringUtils;

/**
 * A utility class to do basics.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    /**
     * Map a given string of the form "word1 word2 word3 ..."
     * into Iterable<Tuple2<K, V>> 
     * where 
     *      K = sorted(word<i>)
     *      V = word<i>
     * 
     * @param line as input String
     * @param N ignore words of size less than N
     * 
     * @return JavaPairRDD<String, String>
     */
    public static Iterable<Tuple2<String, String>> mapToKeyValueList(
            final String line,
            final int N) {
        //
        if ((line == null) || (line.length() < N)) {
            return Collections.EMPTY_LIST;
        }
        //
        String[] words = StringUtils.split(line.toLowerCase());
        if (words == null) {
            return Collections.EMPTY_LIST;
        }
        //
        List<Tuple2<String, String>> list = new ArrayList<>();
        for (String word : words) {
            if (word.length() < N) {
                // ignore strings with less than size N
                continue;
            }
            if (word.matches(".*[,.;]$")) {
                // remove the special char from the end
                word = word.substring(0, word.length() - 1);
            }
            if (word.length() < N) {
                // ignore strings with less than size N
                continue;
            }
            //
            String sortedWord = sort(word);
            list.add(new Tuple2<String, String>(sortedWord, word));
        }
        //
        return list;
    }

    
    /**
     * Sort a single string
     * 
     * @param word a string
     * @return a sorted word
     */
    public static String sort8(final String word) {
        String sorted = word.chars()
                .sorted()
                .collect(StringBuilder::new,
                         StringBuilder::appendCodePoint,
                         StringBuilder::append)
                .toString();
        return sorted;
    }

    /**
     * Sort a single string
     * 
     * @param word a string
     * @return a sorted word
     */
    public static String sort(final String word) {
        char[] chars = word.toCharArray();
        Arrays.sort(chars);
        String sortedWord = String.valueOf(chars);
        return sortedWord;
    }  
    
    /**
     * Merge smaller Map into a larger Map
     * @param smaller a Map
     * @param larger a Map
     * @return merged elements
     */
    public static Map<String, Integer> merge(
            final Map<String, Integer> smaller, 
            final Map<String, Integer> larger) {
        //
        for (String key : smaller.keySet()) {
            Integer frequency = larger.get(key);
            if (frequency == null) {
                larger.put(key, frequency);
            } 
            else {
                larger.put(key, frequency + smaller.get(key));
            }
        }
        //
        return larger;
    }    
    
}
