package org.dataalgorithms.bonus.sortedwordcount.spark;

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
        if (orderBy.equals("ascending")) {
            return frequencies.sortByKey(true);
        }
        else {
            // "descending" order
            return frequencies.sortByKey(false);
        }
    }
    
    static List<String> convertLineToWords(String line, final int N) {
        if ((line == null) || (line.length() < N)) {
            return Collections.emptyList();
        }
        //
        String[] tokens = line.split(" ");
        List<String> list = new ArrayList<>();
        for (String tok : tokens) {
            if (tok.matches(".*[,.;]$")) {
                // remove the special char from the end
                tok = tok.substring(0, tok.length() - 1);
            }
            //
            if (tok.length() < N) {
                continue;
            }
            //
            list.add(tok);
        }
        //
        return list;
    }
    
}
