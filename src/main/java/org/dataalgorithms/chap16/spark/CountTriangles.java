package org.dataalgorithms.chap16.spark;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
//
import scala.Tuple2;
import scala.Tuple3;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
//
import org.dataalgorithms.util.SparkUtil;

/**
 * This class finds, counts, and lists all triangles for a given graph.
 *
 * @author Mahmoud Parsian
 *
 */
public class CountTriangles {
  //
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
       System.err.println("Usage: CountTriangles <input-path> <output-path>");
       System.exit(1);
    }
    String inputPath = args[0];
    String outputPath = args[1];

    // create context object and the first RDD from input-path
    JavaSparkContext ctx = SparkUtil.createJavaSparkContext("count-triangles");
    JavaRDD<String> lines = ctx.textFile(inputPath);

    // PairFlatMapFunction<T, K, V>	
    // T => Iterable<Tuple2<K, V>>
    JavaPairRDD<Long,Long> edges = lines.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
      @Override
      public Iterator<Tuple2<Long,Long>> call(String s) {
         String[] nodes = s.split(" ");
         long start = Long.parseLong(nodes[0]);
         long end = Long.parseLong(nodes[1]);
         // Note that edges must be reciprocal, that 
         // is every {source, destination} edge must have 
         // a corresponding {destination, source}.
        return Arrays.asList(new Tuple2<Long, Long>(start, end), 
                             new Tuple2<Long, Long>(end, start)).iterator();
      }
    });
    
    // form triads
    JavaPairRDD<Long, Iterable<Long>> triads = edges.groupByKey();    
    
    // debug1
    List<Tuple2<Long, Iterable<Long>>> debug1 = triads.collect();
    for (Tuple2<Long, Iterable<Long>> t2 : debug1) {
      System.out.println("debug1 t2._1="+t2._1);
      System.out.println("debug1 t2._2="+t2._2);
    }

    JavaPairRDD<Tuple2<Long,Long>, Long> possibleTriads = 
          triads.flatMapToPair(new PairFlatMapFunction<  
                                                        Tuple2<Long, Iterable<Long>>, // input
                                                        Tuple2<Long,Long>,            // key (output)
                                                         Long                         // value (output)
                                                      >() {
        public Iterator<Tuple2<Tuple2<Long,Long>, Long>> call(Tuple2<Long, Iterable<Long>> s) {
      
           // s._1 = Long (as a key)
           // s._2 = Iterable<Long> (as a values)
           Iterable<Long> values = s._2;
           // we assume that no node has an ID of zero
           List<Tuple2<Tuple2<Long,Long>, Long>> result = new ArrayList<Tuple2<Tuple2<Long,Long>, Long>>();

           // Generate possible triads.
           for (Long value : values) {
              Tuple2<Long,Long> k2 = new Tuple2<Long,Long>(s._1, value);
              Tuple2<Tuple2<Long,Long>, Long> k2v2 = new Tuple2<Tuple2<Long,Long>, Long>(k2, 0l);
              result.add(k2v2);
           }

           // RDD's values are immutable, so we have to copy the values
           // copy values to valuesCopy
           List<Long> valuesCopy = new ArrayList<Long>();
           for (Long item : values) {
              valuesCopy.add(item);
           }
           Collections.sort(valuesCopy);

           // Generate possible triads.
           for (int i=0; i< valuesCopy.size() -1; ++i) {
              for (int j=i+1; j< valuesCopy.size(); ++j) {
                 Tuple2<Long,Long> k2 = new Tuple2<Long,Long>(valuesCopy.get(i), valuesCopy.get(j));
                 Tuple2<Tuple2<Long,Long>, Long> k2v2 = new Tuple2<Tuple2<Long,Long>, Long>(k2, s._1);
                 result.add(k2v2);
              }
           }
     
           return result.iterator();
        }
    });
    
    List<Tuple2<Tuple2<Long,Long>, Long>> debug2 = possibleTriads.collect();
    for (Tuple2<Tuple2<Long,Long>, Long> t2 : debug2) {
      System.out.println("debug2 t2._1="+t2._1);
      System.out.println("debug2 t2._2="+t2._2);
    }
    

    JavaPairRDD<Tuple2<Long,Long>, Iterable<Long>> triadsGrouped = possibleTriads.groupByKey();         
    List<Tuple2<Tuple2<Long,Long>, Iterable<Long>>> debug3 = triadsGrouped.collect();
    for (Tuple2<Tuple2<Long,Long>, Iterable<Long>> t2 : debug3) {
      System.out.println("debug3 t2._1="+t2._1);
      System.out.println("debug3 t2._2="+t2._2);
    }

    JavaRDD<Tuple3<Long,Long,Long>> trianglesWithDuplicates = 
          triadsGrouped.flatMap(new FlatMapFunction< 
                                                     Tuple2<Tuple2<Long,Long>, Iterable<Long>>,  // input
                                                     Tuple3<Long,Long,Long>                      // output
                                                   >() {
        @Override
        public Iterator<Tuple3<Long,Long,Long>> call(Tuple2<Tuple2<Long,Long>, Iterable<Long>> s) {
      
           // s._1 = Tuple2<Long,Long> (as a key) = "<nodeA><,><nodeB>"
           // s._2 = Iterable<Long> (as a values) = {0, n1, n2, n3, ...} or {n1, n2, n3, ...}
           // note that 0 is a fake node, which does not exist
           Tuple2<Long,Long> key = s._1;
           Iterable<Long> values = s._2;
           // we assume that no node has an ID of zero

           List<Long> list = new ArrayList<Long>();
           boolean haveSeenSpecialNodeZero = false;
           for (Long node : values) {
              if (node == 0) {
                 haveSeenSpecialNodeZero = true;
              }         
              else {
                 list.add(node);
              }         
           }
      
           List<Tuple3<Long,Long,Long>> result = new ArrayList<Tuple3<Long,Long,Long>>();
           if (haveSeenSpecialNodeZero) {
              if (list.isEmpty()) {
                // no triangles found
                 // return null;
                 return result.iterator();
              }
              // emit triangles
              for (long node : list) {         
                 long[] aTraingle = {key._1, key._2, node};
                 Arrays.sort(aTraingle);
                 Tuple3<Long,Long,Long> t3 = new Tuple3<Long,Long,Long>(aTraingle[0], 
                                                                        aTraingle[1], 
                                                                        aTraingle[2]);
                 result.add(t3);
              }
           }
           else {
              // no triangles found
              // return null;
              return result.iterator();
           }
     
           return result.iterator();
        }
    });
    
    System.out.println("=== Triangles with Duplicates ===");
    List<Tuple3<Long,Long,Long>> debug4 = trianglesWithDuplicates.collect();
    for (Tuple3<Long,Long,Long> t3 : debug4) {
      //System.out.println(t3._1 + "," + t3._2+ "," + t3._3);
      System.out.println("t3="+t3);
    }   
    
    // now we have to eliminate duplicate triangles
    JavaRDD<Tuple3<Long,Long,Long>> uniqueTriangles = trianglesWithDuplicates.distinct();
     
    System.out.println("=== Unique Triangles ===");
    List<Tuple3<Long,Long,Long>> output = uniqueTriangles.collect();
    for (Tuple3<Long,Long,Long> t3 : output) {
      //System.out.println(t3._1 + "," + t3._2+ "," + t3._3);
      System.out.println("t3="+t3);
    }
    
    uniqueTriangles.saveAsTextFile(outputPath);

    // done 
    ctx.close();
    
    //
    System.exit(0);
  }
}
