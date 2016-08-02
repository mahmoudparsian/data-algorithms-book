package org.dataalgorithms.chapB05.anagram.sparkwithlambda;

// STEP-0: import required classes and interfaces
import java.util.Map;
import java.util.HashMap;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
//
import org.dataalgorithms.chapB05.anagram.util.Util;


/**
 * Find anagram counts for a given set of documents.
 * For example, if the sample input is comprised of 
 * the following 3 lines:
 * 
 *     Mary and Elvis lives in Detroit army Easter Listen 
 *     silent eaters Death Hated elvis Mary easter Silent
 *     Mary and Elvis are in army Listen Silent detroit
 * 
 * Then the output will be:
 *     
 *     Sorted     Anagrams and Frequencies
 *     =====   -> ========================
 *     (adeht  -> {death=1, hated=1})
 *     (eilnst -> {silent=3, listen=2})
 *     (eilsv  -> {lives=1, elvis=3})
 *     (aeerst -> {eaters=1, easter=2})
 *     (amry   -> {army=2, mary=3})
 * 
 * Since "in", "and", "are", "detroit" don't have an associated anagrams, 
 * they will be filtered out (dropped out):
 * 
 *     in -> null
 *     are -> null
 *     and -> null
 *     Detroit -> null
 *
 * @author Mahmoud Parsian
 *
 */
public class AnagramCountUsingCombineByKey {

    public static void main(String[] args) throws Exception {

        // STEP-1: handle input parameters
        if (args.length != 3) {
            System.err.println("Usage: <N> <input-path> <output-path> ");
            System.exit(1);
        }

        // if a word.length < N, that word will be ignored
        final int N = Integer.parseInt(args[0]);
        System.out.println("args[0]: N=" + N);

        // identify I/O paths
        String inputPath = args[1];
        String outputPath = args[2];
        System.out.println("args[1]: <input-path>=" + inputPath);
        System.out.println("args[2]: <output-path>=" + outputPath);

        // STEP-2: create an instance of JavaSparkContext
        JavaSparkContext ctx = new JavaSparkContext();

        // STEP-3: create an RDD for input
        // input record format:
        //      word1 word2 word3 ...
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // STEP-4: create (K, V) pairs from input
        // where 
        //      K = sorted(word)
        //      V = word
        JavaPairRDD<String, String> rdd = lines.flatMapToPair(
                (String line) -> Util.mapToKeyValueList(line, N).iterator()
        );


        // How to use combineByKey(): to use combineByKey(), you 
        // need to define 3 basic functions f1, f2, f3:
        // and then you invoke it as: combineByKey(f1, f2, f3)
        //    function 1: create a combiner data structure 
        //    function 2: merge a value into a combined data structure
        //    function 3: merge two combiner data structures
        
        
        // function 1: create a combiner data structure         
        // Here, the combiner data structure is a Map<String,Integer>,
        // which keeps track of anagrams and its associated frequencies
        Function<String, Map<String, Integer>> createCombiner
                = (String x) -> {
                    Map<String, Integer> map = new HashMap<>();
                    map.put(x, 1);
                    return map;
        };

        // function 2: merge a value into a combined data structure
        Function2<Map<String, Integer>, String, Map<String, Integer>> mergeValue = 
                (Map<String, Integer> map, String x) -> {
                    Integer frequency = map.get(x);
                    if (frequency == null) {
                        map.put(x, 1);
                    }
                    else {
                        map.put(x, frequency + 1);
                    }
                    return map;
        };

        // function 3: merge two combiner data structures
        Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>> mergeCombiners =
                (Map<String, Integer> map1, Map<String, Integer> map2) -> {
                    if (map1.size() < map2.size()) {
                        return Util.merge(map1, map2);
                    }
                    else {
                        return Util.merge(map1, map2);
                    }
        };

        // STEP-5: create anagrams
        // JavaPairRDD<String, Iterable<String>> anagrams = rdd.groupByKey();
        JavaPairRDD<String, Map<String, Integer>> anagrams = rdd.combineByKey(
                createCombiner,
                mergeValue,
                mergeCombiners);

        //STEP-6: filter out the redundant RDD elements  
        //        
        // now we should filter (k,v) pairs from anagrams RDD:
        // where k is a "sorted word" and v is a Map<String,Integer>
        // if v.size() == 1 then it means that there is no associated
        // anagram for the diven "sorted word".
        //
        // For example our anagrams will have the following RDD entry:
        // (k=Detroit, v=Map.Entry("detroit", 2))
        // since the size of v (i.e., the hash map) is one that will 
        // be dropped out
        //
        // public JavaPairRDD<K,V> filter(Function<Tuple2<K,V>,Boolean> f)
        // Return a new RDD containing only the elements that satisfy a predicate;
        // If a counter (i.e., V) is 0, then exclude them 
        JavaPairRDD<String,Map<String, Integer>> filteredAnagrams = 
            anagrams.filter((Tuple2<String, Map<String, Integer>> entry) -> {
                Map<String, Integer> map = entry._2;
                if (map.size() > 1) {
                    return true; // include
                }
                else {
                    return false; // exclude
                }
            }
        );        
        
        
        // STEP-7: save output
        filteredAnagrams.saveAsTextFile(outputPath);

        // STEP-8: done
        ctx.close();
        System.exit(0);
    }

}
