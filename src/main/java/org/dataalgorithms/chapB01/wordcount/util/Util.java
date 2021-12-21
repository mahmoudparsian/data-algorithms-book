package org.dataalgorithms.chapB01.wordcount.util;

//
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
//
import org.apache.commons.lang3.StringUtils;
//

/**
 * Description: Basic utility class.
 *
 * SparkWordCount: Counting the words if their size is 
 * greater than or equal N, where N > 1.
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    /**
     * Convert this string into words
     *
     * @param str convert this string into words
     * @param N an integer > 0, if a word length is < N, then 
     * ignore that word 
     * 
     * @return a list of words from a given string
     *
     */
    public static List<String> convertStringToWords(
            final String str,
            final int N) {
        //
        if ((str == null) || (str.length() < N)) {
            return Collections.emptyList();
        }
        //
        String[] tokens = StringUtils.split(str, " ");
        if ((tokens == null) || (tokens.length == 0)) {
            return Collections.emptyList();
        }
        //        
        List<String> words = new ArrayList<>();
        for (String word : tokens) {
            //
            if (word.matches(".*[,.;]$")) {
                // remove the special char from the end
                word = word.substring(0, word.length() - 1);
            }
            //
            if (word.length() < N) {
                continue;
            }
            //
            words.add(word);
        }
        //
        return words;
    }

}
