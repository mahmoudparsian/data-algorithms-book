package org.dataalgorithms.chapB02.sortedwordcount.spark;

import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Description:
 *
 *    SortedWordCount: 
 *        1. Counting the words if their size is greater than or equal N, where N > 1.
 *        2. Sort words based on frequencies
 *
 * @author Mahmoud Parsian
 *
 */
public class SortedWordCountWithLambda {

    public static void main(String[] args) throws Exception {
       if (args.length != 4) {
          System.err.println("Usage: SortedWordCountWithLambda <N> <input> <output> <orderBy>");
          System.exit(1);
       }

       // handle input parameters
       final int N = Integer.parseInt(args[0]); // 2, 3, 4, ...
       final String inputPath = args[1];        // input path
       final String outputPath = args[2];       // output path
       final String orderBy = args[3];          // sort "ascending" OR "descending"

       // create a context object, which is used 
       // as a factory for creating new RDDs
       JavaSparkContext ctx = new JavaSparkContext();

       // read input and create the first RDD
       JavaRDD<String> lines = ctx.textFile(inputPath, 1);

       JavaRDD<String> words = lines.flatMap((String line) -> Util.convertLineToWords(line, N).iterator());

       JavaPairRDD<String, Integer> ones = 
               words.mapToPair((String s) -> new Tuple2<String, Integer>(s, 1));
       
       // find the total count for each unique word
       JavaPairRDD<String, Integer> counts = 
            ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);
       
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
       JavaPairRDD<Integer,String> frequencies = 
               counts.mapToPair((Tuple2<String, Integer> s) ->
                       new Tuple2<Integer,String>(s._2, s._1));
        
        // next, sort frequencies by key 
        // JavaPairRDD<K,V> sortByKey(boolean ascending)
        JavaPairRDD<Integer,String> sortedByFreq = Util.sort(frequencies, orderBy);
        
        
       // save the sorted final output 
       sortedByFreq.saveAsTextFile(outputPath);

       // close the context and we are done
       ctx.close();
       
       // done
       System.exit(0);
    }
}
