package org.dataalgorithms.chapB09.charcount.spark.inmapper;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Description:

    CharCountInMapperCombiner: Counting the chars and sorting them using mapPartitions()
 *
 * @author Mahmoud Parsian
 *
 */
public class CharCountInMapperCombiner {

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
        JavaRDD<String> lines = context.textFile(inputPath);
       
       
        // JavaRDD<U> mapPartitions(FlatMapFunction<java.util.Iterator<T>,U> f)
        JavaRDD<Map<Character, Integer>> partitions = lines.mapPartitions(
            new FlatMapFunction<Iterator<String>, Map<Character, Integer>>() {
            @Override
            public Iterator<Map<Character, Integer>> call(Iterator<String> iter) {
                Map<Character, Integer> map = new HashMap<Character, Integer>();
                while (iter.hasNext()) {
                    String record = iter.next();
                    String[] words = record.split(" ");
                    for (String word : words) {
                        char[] arr = word.toLowerCase().toCharArray();
                        for (char c : arr) {
                            Integer count = map.get(c);
                            if (count == null) {
                               map.put(c, 1);
                            } 
                            else {
                               map.put(c, count + 1);
                            }
                        }
                    }
                }
                return Collections.singletonList(map).iterator();
            }
        });


      // find a final frequencies table: aggregate values for the same character key
      Map<Character,Integer> finalMap = new HashMap<Character,Integer>();
      List<Map<Character,Integer>> all = partitions.collect();
      for (Map<Character,Integer> localmap : all) {
          //System.out.println("localmap="+localmap);
          for (Map.Entry<Character,Integer> entry : localmap.entrySet()) {
              Character c = entry.getKey();
              Integer count = finalMap.get(c);
              if (count == null) {
                  finalMap.put(c, entry.getValue());
              }
              else {
                  finalMap.put(c, count+entry.getValue());
              }
          }
      }
    
      // STEP_7: emit final top-10
      for (Map.Entry<Character,Integer> entry : finalMap.entrySet()) {
         System.out.println(entry.getKey() + " : " + entry.getValue());
      }       

      // sort and save the final output 
      // counts.sortByKey().saveAsTextFile(outputPath);
      //JavaPairRDD<Character,Integer> finalRDD = context.parallelize(finalMap);

       // close the context and we are done
       context.close();
       
       System.exit(0);
    }
    
}