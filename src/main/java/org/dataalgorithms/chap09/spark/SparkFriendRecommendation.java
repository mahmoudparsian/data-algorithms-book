package org.dataalgorithms.chap09.spark;

import org.dataalgorithms.util.SparkUtil;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The SparkFriendRecommendation is a Spark program to implement a basic
 * friends recommendation engine between all users.
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkFriendRecommendation {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
       System.err.println("Usage: SparkFriendRecommendation <input-path>");
       System.exit(1);
    }  
    final String friendsInputPath = args[0];
    
    // create the first RDD from input
    JavaSparkContext ctx = SparkUtil.createJavaSparkContext("SparkFriendRecommendation");
    JavaRDD<String> records = ctx.textFile(friendsInputPath, 1);

    // debug0
    List<String> debug1 = records.collect();
    for (String t : debug1) {
      System.out.println("debug1 record="+t);
    }

    // flatMapToPair
    //    <K2,V2> JavaPairRDD<K2,V2> flatMapToPair(PairFlatMapFunction<T,K2,V2> f)
    //    Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.

    // PairFlatMapFunction<T, K, V>
    // T => Iterable<Tuple2<K, V>>
    JavaPairRDD<Long, Tuple2<Long,Long>> pairs =
          //                                      T       K     V
          records.flatMapToPair(new PairFlatMapFunction<String, Long, Tuple2<Long,Long>>() {
      @Override
      public Iterator<Tuple2<Long,Tuple2<Long,Long>>> call(String record) {
         // record=<person><TAB><friend1><,><friend2><,><friend3><,>... 
         String[] tokens = record.split("\t");
         long person = Long.parseLong(tokens[0]);
         String friendsAsString = tokens[1];
         String[] friendsTokenized = friendsAsString.split(",");
         
         List<Long> friends = new ArrayList<Long>();         
         List<Tuple2<Long,Tuple2<Long, Long>>> mapperOutput =
             new ArrayList<Tuple2<Long,Tuple2<Long, Long>>>();         
         for (String friendAsString : friendsTokenized) {
            long toUser = Long.parseLong(friendAsString);
            friends.add(toUser);
            Tuple2<Long,Long> directFriend = T2(toUser, -1L);
            mapperOutput.add(T2(person, directFriend));
         }

         for (int i = 0; i < friends.size(); i++) {
            for (int j = i + 1; j < friends.size(); j++) {
                // possible friend 1
                Tuple2<Long,Long> possibleFriend1 = T2(friends.get(j), person);
                mapperOutput.add(T2(friends.get(i), possibleFriend1));
                // possible friend 2
                Tuple2<Long,Long> possibleFriend2 = T2(friends.get(i), person);
                mapperOutput.add(T2(friends.get(j), possibleFriend2));
            }
         } 
                 
         return mapperOutput.iterator();
      }
    });


    // debug2
    List<Tuple2<Long,Tuple2<Long,Long>>> debug2 = pairs.collect();
    for (Tuple2<Long,Tuple2<Long,Long>> t2 : debug2) {
      System.out.println("debug2 key="+t2._1+"\t value="+t2._2);
    }


    JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> grouped = pairs.groupByKey();

    // debug3
    List<Tuple2<Long, Iterable<Tuple2<Long, Long>>>> debug3 = grouped.collect();
    for (Tuple2<Long, Iterable<Tuple2<Long, Long>>> t2 : debug3) {
      System.out.println("debug3 key="+t2._1+"\t value="+t2._2);
    }
    
    // Find intersection of all List<List<Long>>
    // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
    // Pass each value in the key-value pair RDD through a map function without changing the keys;
    // this also retains the original RDD's partitioning.
    JavaPairRDD<Long, String> recommendations =
        grouped.mapValues(new Function< Iterable<Tuple2<Long, Long>>, // input
                                        String                        // final output
                                      >() {
      @Override
      public String call(Iterable<Tuple2<Long, Long>> values) {
      
        // mutualFriends.key = the recommended friend 
        // mutualFriends.value = the list of mutual friends
        final Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();
        for (Tuple2<Long, Long> t2 : values) {
            final Long toUser = t2._1;
            final Long mutualFriend = t2._2;
            final boolean alreadyFriend = (mutualFriend == -1);

            if (mutualFriends.containsKey(toUser)) {
                if (alreadyFriend) {
                     mutualFriends.put(toUser, null);
                } 
                else if (mutualFriends.get(toUser) != null) {
                        mutualFriends.get(toUser).add(mutualFriend);
                }
            } 
            else {
                if (alreadyFriend) {
                     mutualFriends.put(toUser, null);
                } 
                else {
                     List<Long> list1 = new ArrayList<Long>(Arrays.asList(mutualFriend));
                     mutualFriends.put(toUser, list1);
                }
            }
        }
        return buildRecommendations(mutualFriends);
      }
    });


    // debug4
    List<Tuple2<Long,String>> debug4 = recommendations.collect();
    for (Tuple2<Long,String> t2 : debug4) {
      System.out.println("debug4 key="+t2._1+ "\t value="+t2._2);
    }

    // done
    ctx.close();
    System.exit(0);
  }
  
  static String buildRecommendations(Map<Long, List<Long>> mutualFriends) {
     StringBuilder recommendations = new StringBuilder();
     for (Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
        if (entry.getValue() == null) {
           continue;
        }
        recommendations.append(entry.getKey());
        recommendations.append(" (");
        recommendations.append(entry.getValue().size());
        recommendations.append(": ");         
        recommendations.append(entry.getValue());
        recommendations.append("),");
     }
     return recommendations.toString();
  }
     
  static Tuple2<Long,Long> buildSortedTuple(long a, long b) {
     if (a < b) {
        return new Tuple2<Long, Long>(a,b);
     }
     else {
        return new Tuple2<Long, Long>(b,a);
     }
  }

  static Tuple2<Long,Long> T2(long a, long b) {
     return new Tuple2<Long,Long>(a, b);
  }

  static Tuple2<Long,Tuple2<Long,Long>> T2(long a, Tuple2<Long,Long> b) {
     return new Tuple2<Long,Tuple2<Long,Long>>(a, b);
  }

}

