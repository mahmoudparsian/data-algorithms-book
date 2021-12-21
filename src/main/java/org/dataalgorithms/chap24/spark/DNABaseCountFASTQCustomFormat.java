package org.dataalgorithms.chap24.spark;

// STEP-0: import required classes and interfaces
import scala.Tuple2;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
//
import org.dataalgorithms.chap24.mapreduce.FastqInputFormat;

/**
 * This program reads FASTQ input files and computes DNA base counts.
 *
 * FASTQ format is defined here:
 *      http://en.wikipedia.org/wiki/FASTQ_format
 *      http://maq.sourceforge.net/fastq.shtml
 * 
 * 
 *     // NOTE:
 *     //
 *     //     Each fastqRDD entry is Tule2<LongWritable, Text> where the value 
 *     //     (Tuple2._2) is a concatenation of FASTQ record, which has 4 lines:
 *     //
 *     //     <line1><,;,><line2><,;,><line3><,;,><line4>
 *     //
 *     //     where <line2> is the actual DNA sequence, which we are interested in.
 *     //     We used FastqInputFormat to convert FASTQ data records into a single
 *     //     record, where the lines are delimited by ",;,"
 *    
 *
 * @author Mahmoud Parsian
 *
 */
public class DNABaseCountFASTQCustomFormat {

   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length != 1) {
         System.err.println("Usage: SparkDNABaseCountFASTQ <input-path>");
         System.exit(1);
      }
      final String inputPath = args[0];

      // STEP-2: create an RDD from FASTQ input format 
      JavaSparkContext ctx = new JavaSparkContext();

      // public <K,V,F extends org.apache.hadoop.mapreduce.InputFormat<K,V>> JavaPairRDD<K,V> newAPIHadoopFile(
      //             String path,
      //             Class<F> fClass,
      //             Class<K> kClass,
      //             Class<V> vClass,
      //             org.apache.hadoop.conf.Configuration conf)
      // Get an RDD for a given Hadoop file with an arbitrary new API InputFormat and extra 
      // configuration options to pass to the input format.
      // '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object 
      // for each record, directly caching the returned RDD will create many references to 
      // the same object. If you plan to directly cache Hadoop writable objects, you should 
      // first copy them using a map function.
           
      //// you may partition your data by coalesce()
      ////    public JavaPairRDD<T> coalesce(int N)
      ////    Return a new RDD that is reduced into N partitions.      
      JavaPairRDD<LongWritable ,Text> fastqRDD = ctx.newAPIHadoopFile(
                   inputPath,
                   FastqInputFormat.class,
                   LongWritable.class,
                   Text.class,
                   new Configuration() 
      );  
      
      
      // STEP-3: map partitions 
      // <U> JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>,U> f)
      // Return a new RDD by applying a function to each partition of this RDD.	  
      JavaRDD<Map<Character, Long>> partitions = fastqRDD.mapPartitions(
        new FlatMapFunction<Iterator<Tuple2<LongWritable ,Text>>, Map<Character,Long>>() {
        @Override
        public Iterator<Map<Character,Long>> call(Iterator<Tuple2<LongWritable ,Text>> iter) {
        Map<Character,Long> baseCounts = new HashMap<Character,Long>();
        while (iter.hasNext()) {
             Tuple2<LongWritable ,Text> kv = iter.next();
             // get FASTQ record: comprised of 4 lines            
             String fastqRecord = kv._2.toString(); 
             String[] lines = fastqRecord.split(",;,");
             // 2nd line (i.e., lines[1]) is the DNA sequence
             String sequence = lines[1].toUpperCase();
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