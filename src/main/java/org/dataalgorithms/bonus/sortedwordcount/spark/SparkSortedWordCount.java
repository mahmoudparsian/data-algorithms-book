package org.dataalgorithms.bonus.sortedwordcount.spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

/**
 * Description:
 *
 *    SparkSparkWordCount: 
 *        1. Counting the words if their size is greater than or equal N, where N > 1.
 *        2. Sort words based on frequencies
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkSortedWordCount {

    private SparkSortedWordCount() {
    }

    public static void main(String[] args) throws Exception {
       if (args.length != 4) {
          System.err.println("Usage: SparkSortedWordCount <N> <input> <output> <orderBy>");
          System.exit(1);
       }

       // handle input parameters
       final int N = Integer.parseInt(args[0]); // 2, 3, 4, ...
       final String inputPath = args[1];
       final String outputPath = args[2];
       final String orderBy = args[3]; // "ascending" OR "descending"

       // create a context object, which is used 
       // as a factory for creating new RDDs
       JavaSparkContext ctx = new JavaSparkContext();

       // read input and create the first RDD
       JavaRDD<String> lines = ctx.textFile(inputPath, 1);

       //      output                                            input   output
       JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
          //              output       input
          @Override
          public Iterable<String> call(String s) {
             if ((s == null) || (s.length() < N)) {
                return Collections.emptyList();
             }
             
             String[] tokens = s.split(" ");
             List<String> list = new ArrayList<String>();
             for (String  tok : tokens) {
                if (tok.matches(".*[,.;]$")) {
                   // remove the special char from the end
                   tok = tok.substring(0, tok.length() -1); 
                }
           
                if (tok.length() < N) {
                   continue;
                }
           
                list.add(tok);
             }
             return list;
          }
       });

        //           K       V    
       JavaPairRDD<String, Integer> ones = 
                            //                   input    K       V
               words.mapToPair(new PairFunction<String, String, Integer>() {
         //            K       V             input
         @Override
         public Tuple2<String, Integer> call(String s) {
           //                K       V
           return new Tuple2<String, Integer>(s, 1);
         }
       });
       
       // find the total count for each unique word
       JavaPairRDD<String, Integer> counts = 
            ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) {
             return i1 + i2;
          }
       });
       
       // now, we have (K=word, V=frequency)
       // next we sort this RDD based on the frequency of words
       // so the final output will be:
       // 
       //  F1, W1
       //  F2, W2
       //  ...
       //  Fn, Wn
       //
       //  where F1 > F2 > ... > Fn
       
       // first: swap K with V so that V will be the key and K will be the value
       // note that there is no sort by value: that is why we have to swap K with V
       JavaPairRDD<Integer,String> frequencies = counts.mapToPair(
            new PairFunction<
                             Tuple2<String, Integer>,      // T: input
                             Integer,                      // K: output
                             String                        // V: output
                            >() {
            @Override
            public Tuple2<Integer,String> call(Tuple2<String, Integer> s) {
                return new Tuple2<Integer,String>(s._2, s._1);
            }
        });
        
        // next, sort frequencies by key 
        // JavaPairRDD<K,V> sortByKey(boolean ascending)
        JavaPairRDD<Integer,String> sortedByFreq = sort(frequencies, orderBy);
        
        
       // save the sorted final output 
       sortedByFreq.saveAsTextFile(outputPath);

       // close the context and we are done
       ctx.close();
       System.exit(0);
    }
    
    static JavaPairRDD<Integer,String> sort(JavaPairRDD<Integer,String> frequencies, String orderBy) 
        throws Exception {
        if (orderBy.equals("ascending")) {
            return frequencies.sortByKey(true);
        }
        else {
            // "descending" order
            return frequencies.sortByKey(false);
        }
    }
    
}
