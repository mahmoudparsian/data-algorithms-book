package org.dataalgorithms.chap01.spark;

// STEP-0: import required Java/Spark classes.

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
//
import org.dataalgorithms.util.SparkUtil;
import org.dataalgorithms.util.DataStructures;


/** 
 * SecondarySortUsingCombineByKey class implements the secondary sort design pattern 
 * by using combineByKey().
 *
 *
 * Input:
 *
 *    name, time, value  
 *    x,2,9
 *    y,2,5
 *    x,1,3
 *    y,1,7
 *    y,3,1
 *    x,3,6
 *    z,1,4
 *    z,2,8
 *    z,3,7
 *    z,4,0
 *    p,1,10
 *    p,3,60
 *    p,4,40
 *    p,6,20
 *
 * Output: generate a time-series looking like this:
 *
 *       t1   t2   t3   t4  t5     t6
 *  x => [3,  9,   6]
 *  y => [7,  5,   1]
 *  z => [4,  8,   7,   0]
 *  p => [10, null, 60, 40, null , 20]
 * 
 *  x => [(1,3), (2,9), (3,6)]            where 1 < 2 < 3
 *  y => [(1,7), (2,5), (3,1)]            where 1 < 2 < 3 
 *  z => [(1,4), (2,8), (3,7), (4,0)]     where 1 < 2 < 3 < 4
 *  p => [(1,10), (3,60), (4,40), (6,20)] where 1 < 3 < 4 < 6
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortUsingCombineByKey {

    public static void main(String[] args) throws Exception {

        // STEP-1: read input parameters and validate them
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath=" + inputPath);
        String outputPath = args[1];
        System.out.println("outputPath=" + outputPath);

        // STEP-2: Connect to the Sark master by creating JavaSparkContext object
        final JavaSparkContext ctx = SparkUtil.createJavaSparkContext();

        // STEP-3: Use ctx to create JavaRDD<String>
        //  input record format: <name><,><time><,><value>
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // STEP-4: create (key, value) pairs from JavaRDD<String> where
        // key is the {name} and value is a pair of (time, value).
        // The resulting RDD will be JavaPairRDD<String, Tuple2<Integer, Integer>>.    
        // convert each record into Tuple2(name, time, value)
        // PairFunction<T, K, V>	T => Tuple2(K, V) where K=String and V=Tuple2<Integer, Integer>
        //                                                                                     input   K       V
        System.out.println("===  DEBUG STEP-4 ===");
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) {
                String[] tokens = s.split(","); // x,2,5
                System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                Tuple2<Integer, Integer> timevalue = new Tuple2<Integer, Integer>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timevalue);
            }
        });

        // STEP-5: validate STEP-4, we collect all values from JavaPairRDD<> and print it.    
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        for (Tuple2 t : output) {
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._1);
        }

        // How to use combineByKey(): to use combineByKey(), you 
        // need to define 3 basic functions f1, f2, f3:
        // and then you invoke it as: combineByKey(f1, f2, f3)
        //    function 1: create a combiner data structure 
        //    function 2: merge a value into a combined data structure
        //    function 3: merge two combiner data structures
        
        
        // function 1: create a combiner data structure         
        // Here, the combiner data structure is a SortedMap<Integer,Integer>,
        // which keeps track of (time, value) for a given key
        // Tuple2<Integer, Integer> = Tuple2<time, value>
        // SortedMap<Integer, Integer> = SortedMap<time, value>
        Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> createCombiner
                = new Function<Tuple2<Integer, Integer>, SortedMap<Integer, Integer>>() {
            @Override
            public SortedMap<Integer, Integer> call(Tuple2<Integer, Integer> x) {
                Integer time = x._1;
                Integer value = x._2;
                SortedMap<Integer, Integer> map = new TreeMap<>();
                map.put(time, value);
                return map;
            }
        };

        // function 2: merge a value into a combined data structure
        Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>> mergeValue
                = new Function2<SortedMap<Integer, Integer>, Tuple2<Integer, Integer>, SortedMap<Integer, Integer>>() {
            @Override
            public SortedMap<Integer, Integer> call(SortedMap<Integer, Integer> map, Tuple2<Integer, Integer> x) {
                Integer time = x._1;
                Integer value = x._2;
                map.put(time, value);
                return map;
            }
        };

        // function 3: merge two combiner data structures
        Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>> mergeCombiners
                = new Function2<SortedMap<Integer, Integer>, SortedMap<Integer, Integer>, SortedMap<Integer, Integer>>() {
            @Override
            public SortedMap<Integer, Integer> call(SortedMap<Integer, Integer> map1, SortedMap<Integer, Integer> map2) {
                if (map1.size() < map2.size()) {
                    return DataStructures.merge(map1, map2);
                } else {
                    return DataStructures.merge(map1, map2);
                }
            }
        };

        // STEP-5: create sorted (time, value)
        JavaPairRDD<String, SortedMap<Integer, Integer>> combined = pairs.combineByKey(
                createCombiner,
                mergeValue,
                mergeCombiners);

        // STEP-7: validate STEP-6, we collect all values from JavaPairRDD<> and print it.    
        System.out.println("===  DEBUG STEP-6 ===");
        List<Tuple2<String, SortedMap<Integer, Integer>>> output2 = combined.collect();
        for (Tuple2<String, SortedMap<Integer, Integer>> t : output2) {
            String name = t._1;
            SortedMap<Integer, Integer> map = t._2;
            System.out.println(name);
            System.out.println(map);
        }

        // persist output
        combined.saveAsTextFile(outputPath);

        // done!
        ctx.close();
        
        // exit
        System.exit(0);
    }

}
