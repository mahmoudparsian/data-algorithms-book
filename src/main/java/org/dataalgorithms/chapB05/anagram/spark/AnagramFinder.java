package org.dataalgorithms.chapB05.anagram.spark;

// STEP-0: import required classes and interfaces
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
//
import org.dataalgorithms.chapB05.anagram.util.Util;

/**
 * Find anagrams for a given set of documents.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class AnagramFinder {

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
        // K = sorted(word)
        // V = word        
        JavaPairRDD<String, String> rdd = lines.flatMapToPair(
                new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(String line) {  
                return Util.mapToKeyValueList(line, N).iterator();
            } 
         });

        // STEP-5: create anagrams
        JavaPairRDD<String, Iterable<String>> anagrams = rdd.groupByKey();
        
        
        // use mapValues() to find frequency of anagrams
        //mapValues[U](f: (V) => U): JavaPairRDD[K, U]
        // Pass each value in the key-value pair RDD through a map function without 
        // changing the keys; this also retains the original RDD's partitioning.
        JavaPairRDD<String, Set<String>> anagramsAsSet
                = anagrams.mapValues(
                        new Function< 
                                     Iterable<String>,    // input
                                     Set<String>          // output
                                    >() {
                    @Override
                    public Set<String> call(Iterable<String> values) {
                        Set<String> set = new HashSet<>();
                        for (String v : values) {
                            set.add(v);
                        }
                        return set;
                    }
                });
        
        //STEP-6: filter out the redundant RDD elements  
        //        
        // now we should filter (k,v) pairs from anagrams RDD:
        // For example our anagrams will have the following RDD entry:
        // (k="eilsv", v={"elvis", "lives"})
        // If the size of v is one that will then it will be be dropped out
        //
        // public JavaPairRDD<K,V> filter(Function<Tuple2<K,V>,Boolean> f)
        // Return a new RDD containing only the elements that satisfy a predicate;
        // If a counter (i.e., V) is 0, then exclude them 
        JavaPairRDD<String, Set<String>> filteredAnagrams
                = anagramsAsSet.filter(new Function<Tuple2<String, Set<String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Set<String>> entry) {
                        Set<String> set = entry._2;
                        if (set.size() > 1) {
                            return true; // include
                        } else {
                            return false; // exclude
                        }
                    }
                });

        // STEP-6: save output
        filteredAnagrams.saveAsTextFile(outputPath);

        // STEP-7: done
        ctx.close();
        
        System.exit(0);
    }
}
