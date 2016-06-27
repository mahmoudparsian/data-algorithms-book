package org.dataalgorithms.bonus.anagram.spark;

// STEP-0: import required classes and interfaces
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
//
import org.apache.commons.lang.StringUtils;

/**
 * Find anagrams for a given set of documents.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    // create (K, V) pairs from input
    // K = sorted(word)
    // V = word
    public static JavaPairRDD<String, String> mapToKeyValue(
            final JavaRDD<String> lines,
            final int N)
            throws Exception {
        //
        JavaPairRDD<String, String> rdd = lines.flatMapToPair(
                new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String line) {
                if ((line == null) || (line.length() < N)) {
                    return Collections.EMPTY_LIST;
                }
                //
                String[] words = StringUtils.split(line.toLowerCase());
                if (words == null) {
                    return Collections.EMPTY_LIST;
                }
                //
                List<Tuple2<String, String>> results = new ArrayList<>();
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
                    results.add(new Tuple2<String, String>(sortedWord, word));
                }
                return results;
            }
        });
        //
        return rdd;
    }
    
    static String sort8(final String word) {
        String sorted = word.chars()
                .sorted()
                .collect(StringBuilder::new,
                         StringBuilder::appendCodePoint,
                         StringBuilder::append)
                .toString();
        return sorted;
    }

    static String sort(final String word) {
        char[] chars = word.toCharArray();
        Arrays.sort(chars);
        String sortedWord = String.valueOf(chars);
        return sortedWord;
    }    
}
