package org.dataalgorithms.chap08.sparkwithlambda;

import scala.Tuple2;
//
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//
import org.dataalgorithms.util.SparkUtil;


/**
 * The FindCommonFriends is a Spark program to find "common friends"
 * between all users.
 *
 * @author Mahmoud Parsian
 *
 */
public class FindCommonFriends {

  public static void main(String[] args) throws Exception {
  
    if (args.length < 1) {
       System.err.println("Usage: FindCommonFriends <input-file>");
       System.exit(1);
    }
    
    
    String hdfsInputFileName = args[0];
    System.out.println("hdfsInputFileName="+hdfsInputFileName);

    // create context object
    JavaSparkContext ctx = SparkUtil.createJavaSparkContext("FindCommonFriends");

    // create the first RDD from input file
    JavaRDD<String> records = ctx.textFile(hdfsInputFileName, 1);

    // debug0 
    List<String> debug0 = records.collect();
    for (String t : debug0) {
      System.out.println("debug0 record="+t);
    }


    // PairFlatMapFunction<T, K, V>
    // T => Iterable<Tuple2<K, V>>
    JavaPairRDD<Tuple2<Long,Long>,Iterable<Long>> pairs = 
          records.flatMapToPair((String s) -> {
              String[] tokens = s.split(",");
              long person = Long.parseLong(tokens[0]);
              String friendsAsString = tokens[1];
              String[] friendsTokenized = friendsAsString.split(" ");
              if (friendsTokenized.length == 1) {
                  Tuple2<Long,Long> key = buildSortedTuple(person, Long.parseLong(friendsTokenized[0]));
                  List<Tuple2<Tuple2<Long, Long> ,Iterable<Long>>> list =
                          Arrays.asList(new Tuple2<Tuple2<Long,Long>,Iterable<Long>>(key, new ArrayList<Long>()));
                  return list.iterator();
              }
              List<Long> friends = new ArrayList<Long>();
              for (String f : friendsTokenized) {
                  friends.add(Long.parseLong(f));
              }
              
              List<Tuple2<Tuple2<Long, Long> ,Iterable<Long>>> result =
                      new ArrayList<Tuple2<Tuple2<Long, Long> ,Iterable<Long>>>();
              for (Long f : friends) {
                  Tuple2<Long,Long> key = buildSortedTuple(person, f);
                  result.add(new Tuple2<Tuple2<Long,Long>, Iterable<Long>>(key, friends));
              }
              return result.iterator();
          }
    );

    // debug1
    List<Tuple2<Tuple2<Long, Long> ,Iterable<Long>>> debug1 = pairs.collect();
    for (Tuple2<Tuple2<Long,Long>,Iterable<Long>> t2 : debug1) {
      System.out.println("debug1 key="+t2._1+"\t value="+t2._2);
    }
    
    JavaPairRDD<Tuple2<Long, Long>, Iterable<Iterable<Long>>> grouped = pairs.groupByKey();  

    // debug2
    List<Tuple2<Tuple2<Long, Long> ,Iterable<Iterable<Long>>>> debug2 = grouped.collect();
    for (Tuple2<Tuple2<Long,Long>, Iterable<Iterable<Long>>> t2 : debug2) {
      System.out.println("debug2 key="+t2._1+"\t value="+t2._2);
    }

    // Find intersection of all List<List<Long>>
    // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
    // Pass each value in the key-value pair RDD through a map function without changing the keys; 
    // this also retains the original RDD's partitioning.
    JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> commonFriends = 
        grouped.mapValues((Iterable<Iterable<Long>> s) -> {
            Map<Long, Integer> countCommon = new HashMap<Long, Integer>();
            int size = 0;
            for (Iterable<Long> iter : s) {
                size++;
                List<Long> list = iterableToList(iter);
                if ((list == null) || (list.isEmpty())) {
                    continue;
                }
                //
                for (Long f : list) {
                    Integer count = countCommon.get(f);
                    if (count == null) {
                        countCommon.put(f, 1);
                    }
                    else {
                        countCommon.put(f, ++count);
                    }
                }
            }
            
            // if countCommon.Entry<f, count> ==  countCommon.Entry<f, s.size()>
            // then that is a common friend
            List<Long> finalCommonFriends = new ArrayList<Long>();
            for (Map.Entry<Long, Integer> entry : countCommon.entrySet()){
                if (entry.getValue() == size) {
                    finalCommonFriends.add(entry.getKey());
                }
            }
            return finalCommonFriends;
        } 
    );
   
    // debug3
    List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> debug3 = commonFriends.collect();
    for (Tuple2<Tuple2<Long, Long>, Iterable<Long>> t2 : debug3) {
      System.out.println("debug3 key="+t2._1+ "\t value="+t2._2);
    }
    
    System.exit(0);
  }
  
  static Tuple2<Long,Long> buildSortedTuple(long a, long b) {
     if (a < b) {
        return new Tuple2<Long, Long>(a,b);
     }
     else {
        return new Tuple2<Long, Long>(b,a);
     }
  }

  static List<Long> iterableToList(Iterable<Long> iterable) {
    List<Long> list = new ArrayList<Long>(); 
    for (Long item : iterable) {      
       list.add(item);
    }
    return list;
  }
  
}
