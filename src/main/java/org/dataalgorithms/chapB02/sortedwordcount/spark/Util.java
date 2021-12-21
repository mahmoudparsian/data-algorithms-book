package org.dataalgorithms.chapB02.sortedwordcount.spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
//
import org.apache.spark.api.java.JavaPairRDD;

/**
 * 
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {
    
    static JavaPairRDD<Integer,String> sort(
            final JavaPairRDD<Integer,String> frequencies, 
            final String orderBy) 
        throws Exception {
        //
        if (orderBy.equals("ascending")) {
            // sort in "ascending" order
            return frequencies.sortByKey(true);
        }
        else {
            // sort in "descending" order
            return frequencies.sortByKey(false);
        }
    }
    
    static List<String> convertLineToWords(String line, final int N) {
        //
        if ((line == null) || (line.length() < N)) {
            return Collections.emptyList();
        }
        //
        String[] words = line.split(" ");
        if ((words == null) || (words.length < 1)) {
            return Collections.emptyList();
        }
        //
        List<String> list = new ArrayList<>();
        for (String word : words) {
            if (word.matches(".*[,.;]$")) {
                // remove the special char from the end
                word = word.substring(0, word.length() - 1);
            }
            //
            if (word.length() < N) {
                continue;
            }
            //
            list.add(word);
        }
        //
        return list;
    }
    
}
