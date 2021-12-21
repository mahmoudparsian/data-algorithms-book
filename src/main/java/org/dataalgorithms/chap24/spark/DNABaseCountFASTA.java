package org.dataalgorithms.chap24.spark;

// STEP-0: import required classes and interfaces
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * This program reads FASTA input files and computes DNA base counts.
 *
 * FASTA format is defined here: 
 *      http://en.wikipedia.org/wiki/FASTA_format
 *      http://www.ncbi.nlm.nih.gov/BLAST/blastcgihelp.shtml
 *
 * @author Mahmoud Parsian
 *
 */
public class DNABaseCountFASTA {

    public static void main(String[] args) throws Exception {
        // STEP-1: handle input parameters
        if (args.length != 1) {
            System.err.println("Usage: SparkDNABaseCountFASTA <input-path>");
            System.exit(1);
        }
        
        // read input path from command line
        final String inputPath = args[0];

        // STEP-2: create an RDD from FASTA input format 
        JavaSparkContext ctx = new JavaSparkContext();
        JavaRDD<String> fastaRDD = ctx.textFile(inputPath);
        
        //// you may partition your data by coalesce()
        ////    public JavaRDD<T> coalesce(int N)
        ////    Return a new RDD that is reduced into N partitions.      

        // STEP-3: map partitions 
        // <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>,U> f)
        // Return a new RDD by applying a function to each partition of this RDD.	  
        JavaRDD<Map<Character, Long>> partitions = fastaRDD.mapPartitions(
                new FlatMapFunction<Iterator<String>, Map<Character, Long>>() {
            @Override
            public Iterator<Map<Character, Long>> call(Iterator<String> iter) {
                Map<Character, Long> baseCounts = new HashMap<Character, Long>();
                while (iter.hasNext()) {
                    String record = iter.next();
                    if (record.startsWith(">")) {
                        // it is a FASTA comment record, ignore it
                        continue;
                    }

                    // otherwise it is a DNA Sequence
                    String str = record.toUpperCase();
                    for (int i = 0; i < str.length(); i++) {
                        char c = str.charAt(i);
                        Long count = baseCounts.get(c);
                        if (count == null) {
                            baseCounts.put(c, 1l);
                        } 
                        else {
                            baseCounts.put(c, count + 1l);
                        }
                    }
                }
                return Collections.singletonList(baseCounts).iterator();
            }
        });

        // STEP-4: collect all DNA base counts
        // List<Map<Character, Long>> list = partitions.collect();
        Map<Character, Long> allBaseCounts = Util.merge(partitions.collect());

        // STEP-5: emit/debug final counts
        for (Map.Entry<Character, Long> entry : allBaseCounts.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }

        // done
        ctx.close();

        System.exit(0);
    }
}
