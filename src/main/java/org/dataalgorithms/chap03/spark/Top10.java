package org.dataalgorithms.chap03.spark;

// STEP-0: import required classes and interfaces
import org.dataalgorithms.util.SparkUtil;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Iterator;
import java.util.Collections;

/**
 * Assumption: for all input (K, V), K's are unique.
 * This means that there will not etries like (A, 5) and (A, 8).
 *
 * This class implements Top-N design pattern for N > 0.
 * This class may be used to find bottom-N as well (by 
 * just keeping N-smallest elements in the set.
 * 
 *  Top-10 Design Pattern: “Top Ten” Structure 
 * 
 *    class mapper : 
 *         setup(): initialize top ten sorted list 
 *         map(key, record ): 
 *                       Insert record into top ten sorted list if length of array 
 *                       is greater than 10.
 *                       Truncate list to a length of 10.
 *         cleanup() : for record in top sorted ten list: emit null, record 
 *
 *    class reducer: 
 *               setup(): initialize top ten sorted list 
 *               reduce(key, records): sort records 
 *                                     truncate records to top 10 
 *                                     for record in records: emit record 
 *
 * @author Mahmoud Parsian
 *
 */
public class Top10 {

   public static void main(String[] args) throws Exception {
  
      // STEP-1: handle input parameters
      if (args.length < 1) {
         System.err.println("Usage: Top10 <input-file>");
         System.exit(1);
      }
      String inputPath = args[0];
      System.out.println("args[0]: <input-path>="+inputPath);

      // STEP-2: create an instance of JavaSparkContext
      JavaSparkContext ctx = SparkUtil.createJavaSparkContext();

      // STEP-3: create an RDD for input
      // input record format:
      //  <string-key><,><integer-value>,
      JavaRDD<String> lines = ctx.textFile(inputPath, 1);

    
      // STEP-4: create (K, V) pairs
      // Note: the assumption is that all K's are unique
      // PairFunction<T, K, V>
      // T => Tuple2<K, V>
      //                                                                    input   K       V
      JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
         @Override
         public Tuple2<String,Integer> call(String s) {
            String[] tokens = s.split(","); // cat7,234
            return new Tuple2<String,Integer>(tokens[0], Integer.parseInt(tokens[1]));
         }
      });

      List<Tuple2<String,Integer>> debug1 = pairs.collect();
      for (Tuple2<String,Integer> t2 : debug1) {
         System.out.println("key="+t2._1 + "\t value= " + t2._2);
      }

    
      // STEP-5: create a local top-10
      JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(
         new FlatMapFunction<Iterator<Tuple2<String,Integer>>, SortedMap<Integer, String>>() {
         @Override
         public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String,Integer>> iter) {
             SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
             while (iter.hasNext()) {
                Tuple2<String,Integer> tuple = iter.next();
                top10.put(tuple._2, tuple._1);
                // keep only top N 
                if (top10.size() > 10) {
                   top10.remove(top10.firstKey());
                }  
             }
             return Collections.singletonList(top10).iterator();
         }
      });

      // STEP-6: find a final top-10
      SortedMap<Integer, String> finaltop10 = new TreeMap<Integer, String>();
      List<SortedMap<Integer, String>> alltop10 = partitions.collect();
      for (SortedMap<Integer, String> localtop10 : alltop10) {
          //System.out.println(tuple._1 + ": " + tuple._2);
          // weight/count = tuple._1
          // catname/URL = tuple._2
          for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
              //   System.out.println(entry.getKey() + "--" + entry.getValue());
              finaltop10.put(entry.getKey(), entry.getValue());
              // keep only top 10 
              if (finaltop10.size() > 10) {
                 finaltop10.remove(finaltop10.firstKey());
              }
          }
      }
    
      // STEP_7: emit final top-10
      for (Map.Entry<Integer, String> entry : finaltop10.entrySet()) {
         System.out.println(entry.getKey() + "--" + entry.getValue());
      }

      System.exit(0);
   }
}
