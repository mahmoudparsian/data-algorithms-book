package org.dataalgorithms.bonus.anagram.spark;

// STEP-0: import required classes and interfaces
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import java.util.Arrays;

/**
 * Find anagrams for a given set of documents.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkAnagram {

   public static void main(String[] args) throws Exception {
  
      // STEP-1: handle input parameters
      if (args.length != 3) {
         System.err.println("Usage: <N> <input-path> <output-path> ");
         System.exit(1);
      }
      
      // if a word.length < N, that word will be ignored
      final int N = Integer.parseInt(args[0]);
      System.out.println("args[0]: N="+N);
      
      // identify I/O paths
      String inputPath = args[1];
      String outputPath = args[2];
      System.out.println("args[1]: <input-path>="+inputPath);
      System.out.println("args[2]: <output-path>="+outputPath);

      // STEP-2: create an instance of JavaSparkContext
      JavaSparkContext ctx = new JavaSparkContext();

      // STEP-3: create an RDD for input
      // input record format:
      //      word1 word2 word3 ...
      JavaRDD<String> lines = ctx.textFile(inputPath, 1);

    
      // STEP-4: create (K, V) pairs from input
      // K = sorted(word)
      // V = word
      JavaPairRDD<String, String> rdd = lines.flatMapToPair(
          new PairFlatMapFunction<String, String, String>() {
          @Override
          public Iterable<Tuple2<String, String>> call(String line) {
             if ((line == null) || (line.length() < N)) {
               return Collections.EMPTY_LIST;
             }
        
             String[] words = StringUtils.split(line);
             if (words == null) {
               return Collections.EMPTY_LIST;
             }
        
             List<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
             for (String word : words) {
                 if (word.length() < N) {
                    // ignore strings with less than size N
                    continue;
                 }
                 if (word.matches(".*[,.;]$")) {
                    // remove the special char from the end
                    word = word.substring(0, word.length() -1); 
                 }
                 if (word.length() < N) {
                    // ignore strings with less than size N
                    continue;
                 }
            
                 char[] wordChars = word.toCharArray();
                 Arrays.sort(wordChars);
                 String sortedWord = new String(wordChars);
                 results.add(new Tuple2<String,String>(sortedWord, word));
            }
            return results;
         }
      });
    
      // STEP-5: create anagrams
      JavaPairRDD<String, Iterable<String>> anagrams = rdd.groupByKey();
      
      // STEP-6: save output
      anagrams.saveAsTextFile(outputPath);

      // STEP-7: done
      ctx.close();
      System.exit(0);
   }
}
