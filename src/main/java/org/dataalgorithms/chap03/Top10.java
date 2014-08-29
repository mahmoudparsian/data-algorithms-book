package org.dataalgorithms.chap03;

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
  
    if (args.length < 2) {
	   // Spark master URL:
	   //	  format:   spark://<spark-master-host-name>:7077
	   //	  example:  spark://myserver00:7077
       System.err.println("Usage: Top10 <spark-master-URL> <file>");
       System.exit(1);
    }
    System.out.println("args[0]: <spark-master-URL>="+args[0]);
    System.out.println("args[1]: <file>="+args[1]);


    JavaSparkContext ctx = SparkUtil.createJavaSparkContext(args[0], "Top10");

    // input record format:
    //  <integer-value>,<string-key>
    JavaRDD<String> lines = ctx.textFile(args[1], 1);

    
    // PairFunction<T, K, V>	T => Tuple2<K, V>
    //                                                                    input  output   output
    JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
      public Tuple2<Integer, String> call(String s) {
	    String[] tokens = s.split(","); // 10,cat10
        return new Tuple2<Integer, String>(Integer.parseInt(tokens[0]), tokens[1]);
      }
    });

    List<Tuple2<Integer,String>> debug1 = pairs.collect();
    for (Tuple2<Integer,String> t2 : debug1) {
      System.out.println("key="+t2._1 + "\t value= " + t2._2);
    }

    
    // create a local top-10
	JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(
	  new FlatMapFunction<Iterator<Tuple2<Integer, String>>, SortedMap<Integer, String>>() {
		@Override
		public Iterable<SortedMap<Integer, String>> call(Iterator<Tuple2<Integer, String>> iter) {
		  SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
		  while (iter.hasNext()) {
		     Tuple2<Integer, String> tuple = iter.next();
             top10.put(tuple._1, tuple._2);
             // keep only top N 
             if (top10.size() > 10) {
                top10.remove(top10.firstKey());
             }      			 
		  }
		  return Collections.singletonList(top10);
		}
	});


    SortedMap<Integer, String> finaltop10 = new TreeMap<Integer, String>();
    List<SortedMap<Integer, String>> alltop10 = partitions.collect();
    for (SortedMap<Integer, String> localtop10 : alltop10) {
        //System.out.println(tuple._1 + ": " + tuple._2);
        // weight = tuple._1
        // catname = tuple._2
        for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
            //   System.out.println(entry.getKey() + "--" + entry.getValue());
            finaltop10.put(entry.getKey(), entry.getValue());
            // keep only top 10 
            if (finaltop10.size() > 10) {
               finaltop10.remove(finaltop10.firstKey());
            }
        }
    }
    
    for (Map.Entry<Integer, String> entry : finaltop10.entrySet()) {
       System.out.println(entry.getKey() + "--" + entry.getValue());
    }

    System.exit(0);
  }

}
