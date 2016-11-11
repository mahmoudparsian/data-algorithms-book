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
 * This program reads FASTQ input files and computes DNA base counts.
 *
 * FASTQ format is defined here:
 *      http://en.wikipedia.org/wiki/FASTQ_format
 *      http://maq.sourceforge.net/fastq.shtml
 *
 * @author Mahmoud Parsian
 *
 */
public class DNABaseCountFASTQTextFormat {

   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length != 1) {
         System.err.println("Usage: DNABaseCountFASTQTextFormat <input-path>");
         System.exit(1);
      }
      final String inputPath = args[0];

      // STEP-2: create an RDD from FASTQ input format 
      JavaSparkContext ctx = new JavaSparkContext();
      JavaRDD<String> fastqRDD = ctx.textFile(inputPath);
      
         
      // STEP-3: map partitions 
      // <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>,U> f)
      // Return a new RDD by applying a function to each partition of this RDD.	  
      JavaRDD<Map<Character, Long>> partitions = fastqRDD.mapPartitions(
        new FlatMapFunction<Iterator<String>, Map<Character,Long>>() {
        @Override
        public Iterator<Map<Character,Long>> call(Iterator<String> iter) {
        Map<Character,Long> baseCounts = new HashMap<Character,Long>();
        while (iter.hasNext()) {
             // get FASTQ record: comprised of 4 lines            
             String fastqRecord = iter.next(); 
             if (fastqRecord.startsWith("@") || // line 1 of FASTQ, ignore
                 fastqRecord.startsWith("+") || // line 3 of FASTQ, ignore
                 fastqRecord.startsWith(";") || // line 4 of FASTQ, ignore
                 fastqRecord.startsWith("!") || // line 4 of FASTQ, ignore
                 fastqRecord.startsWith("~")) {
                 continue;
             }            
             //
             // line 2 of FASTQ is the actual DNA Sequence
             String sequence = fastqRecord.toUpperCase();
             for (int i = 0; i < sequence.length(); i++){
                char c = sequence.charAt(i);
                Long count = baseCounts.get(c);
                if (count == null) {
                    baseCounts.put(c, 1l);
                }
                else {
                    baseCounts.put(c, count+1l);
                }
             }     
          }
          return Collections.singletonList(baseCounts).iterator();
        }
      });

      // STEP-4: collect all DNA base counts
      // List<Map<Character, Long>> list = partitions.collect();
      Map<Character, Long> allBaseCounts = Util.merge(partitions.collect());

      // STEP-5: emit final counts
      for (Map.Entry<Character, Long> entry : allBaseCounts.entrySet()) {
         System.out.println(entry.getKey() + "\t" + entry.getValue());
      }
      
      // done
      ctx.close();

      System.exit(0);
   }
}
