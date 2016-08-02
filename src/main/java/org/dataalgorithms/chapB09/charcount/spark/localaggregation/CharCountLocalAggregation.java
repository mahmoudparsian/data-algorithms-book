package org.dataalgorithms.chapB09.charcount.spark.localaggregation;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

/**
 * Description:
 *
 *    SparkCharCount: Counting the chars and sorting them.
 *
 * @author Mahmoud Parsian
 *
 */
public class CharCountLocalAggregation {

    public static void main(String[] args) throws Exception {
       if (args.length != 2) {
          System.err.println("Usage: SparkCharCount <input> <output>");
          System.exit(1);
       }

       // handle input parameters
       final String inputPath = args[0];
       final String outputPath = args[1];

       // create a context object, which is used 
       // as a factory for creating new RDDs
       JavaSparkContext context = new JavaSparkContext();

       // read input and create the first RDD
       JavaRDD<String> lines = context.textFile(inputPath, 1);

       //                                                                              input   output:K   output:V
       JavaPairRDD<Character,Long> chars = lines.flatMapToPair(new PairFlatMapFunction<String, Character, Long>() {
          @Override
          public Iterator<Tuple2<Character,Long>> call(String s) {
             if ((s == null) || (s.length() == 0)) {
                return Collections.EMPTY_LIST.iterator();
             }            
             Map<Character,Long> map = new HashMap<Character,Long>();
             String[] words = s.split(" ");
             for (String  word : words) {
                char[] arr = word.toLowerCase().toCharArray();
                for (char c : arr) {
                    Long count = map.get(c);
                    if (count == null) {
                        map.put(c, 1l);
                    }
                    else {
                        map.put(c, count+1);
                    }
                }
             }
             return toListOfKeyValuePairs(map).iterator();
          }
       });
 

       // find the total count for each unique char
       JavaPairRDD<Character, Long> counts = 
            chars.reduceByKey(new Function2<Long, Long, Long>() {
          @Override
          public Long call(Long i1, Long i2) {
             return i1 + i2;
          }
       });

       // sort and save the final output 
       counts.sortByKey().saveAsTextFile(outputPath);

       // close the context and we are done
       context.close();
       System.exit(0);
    }
    
    private static List<Tuple2<Character,Long>> toListOfKeyValuePairs(Map<Character,Long> map) {
        List<Tuple2<Character,Long>> list = new ArrayList<Tuple2<Character,Long>>();
        for (Map.Entry<Character,Long> entry : map.entrySet()) {
            list.add(new Tuple2<Character, Long>(entry.getKey(), entry.getValue()));
        }
        return list;
    }
    
}