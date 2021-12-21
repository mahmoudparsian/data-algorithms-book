package org.dataalgorithms.chapB10.friendrecommendation.spark;


// STEP-0: import required classes and interfaces
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.lang.StringUtils;

/**
 * Given the following input:
 * 
 *    <person><:><friend1><,><friend2><,>...
 *
 *     where <friend1>, <friend2>, ... are direct friends of <person>
 *      and note that <person>, <friend1>, <friend2>, ... are userID's 
 *      of java.lang.Long data type 
 *
 * 
 * The goal is to find a set of recommendations for each user; 
 * output will be in the following format:
 *
 *  <person>  <friend-recommendation-1><,><friend-recommendation-2>...
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkFriendRecommendation {

   /**
    * Build a pair of Longs(x,y) where x <= y
    */
   private static Tuple2<Long,Long> buildSortedPairOfLongs(long a, long b) {
       if (a <= b) {
           return new Tuple2<Long,Long>(a, b);
       }
       else {
           return new Tuple2<Long,Long>(b, a);
       }
   }

   public static void main(String[] args) throws Exception {

      // STEP-1: handle input parameters
      if (args.length < 3) {
         System.err.println("Usage: <maxNumberOfRecommendations> <input-path> <output-path> [yarn-cluster]");
         System.exit(1);
      }

      // only maxNumberOfRecommendations will be reported per user
      final int maxNumberOfRecommendations = Integer.parseInt(args[0]);
      System.out.println("args[0]: maxNumberOfRecommendations="+maxNumberOfRecommendations);

      // identify I/O paths
      String inputPath = args[1];
      String outputPath = args[2];
      System.out.println("args[1]: <input-path>="+inputPath);
      System.out.println("args[2]: <output-path>="+outputPath);

      // STEP-2: create an instance of JavaSparkContext
      JavaSparkContext ctx = null;
      if (args.length == 3) {
         ctx = new JavaSparkContext();
      }
      else if (args.length == 4) {
         // args[3] = "yarn-cluster"
         // this is needed if you want to launch this from a Java-client 
         // rather than a shell script (.../bin/spark-submit)
         ctx = new JavaSparkContext("yarn-cluster", "SparkFriendRecommendation");
      }

      // STEP-3: create an RDD for input
      // input record format:
      //   <person><:><friend1><,><friend2><,>...
      JavaRDD<String> lines = ctx.textFile(inputPath, 1);


      // STEP-4: create (K, V) pairs from input
      // K = Tuple2<Long,Long> = Tuple2(user1,user2)
      // V = Long = 0 if diret friend, 1 if 2nd-degree friend
      JavaPairRDD<Tuple2<Long,Long>, Long> rdd = lines.flatMapToPair(
          new PairFlatMapFunction<String, Tuple2<Long,Long>, Long>() {
          @Override
          public Iterator<Tuple2<Tuple2<Long,Long>, Long>> call(String record) {       
             if ((record == null) || (record.length() == 0)) {
                return Collections.EMPTY_LIST.iterator();
             }
             String[] tokens = StringUtils.split(record, ":");
            long person = Long.parseLong(tokens[0]);
            //System.out.println("person="+person);
            String friendsAsCSV = tokens[1];
            String[] friendsAsArray = StringUtils.split(friendsAsCSV, ",");
            //debug(friendsAsArray);
       
            // add all friends to a list    
            List<Long> friends =  new ArrayList<Long>();
            for (String friendAsString : friendsAsArray) {
               long friend = Long.parseLong(friendAsString);
               friends.add(friend);
            }

            // sort friends IDs
            Collections.sort(friends);
            //System.out.println("friends="+friends);

            List<Tuple2<Tuple2<Long,Long>, Long>> kvPairs = 
                new ArrayList<Tuple2<Tuple2<Long,Long>, Long>>();
            for (int i = 0; i < friends.size(); i++) {
               long f1 = friends.get(i);

               // create a key representing the user and direct friend
               // assure that the lower of user and f1 is first in the key
               // this is a direct (first degree) connection, therefore we
               // flag this connection by using the zero flag
               Tuple2<Long,Long> s1 = buildSortedPairOfLongs(person, f1);
               kvPairs.add(new Tuple2<Tuple2<Long,Long>, Long>(s1, 0l));
               //System.out.println("s1="+s1.toString()+"0");

               for (int j = i+1; j < friends.size(); j++) {
                  long f2 = friends.get(j);
                  // (f1, f2) represents 2nd-degree of connection
                  Tuple2<Long,Long> s2 = new Tuple2<Long,Long>(f1, f2);

                  // f1 is always <= f2 because we have sorted the friends
                  // the s2 connection represents one 2nd-degree of connection, 
                  // therefore, we use a ONE to represent one connection
                  kvPairs.add(new Tuple2<Tuple2<Long,Long>, Long>(s2, 1l));
               }
            } 
            return kvPairs.iterator();
          }
      });
      
      

      // STEP-5: group (user1,user2) 
      JavaPairRDD<Tuple2<Long,Long>, Iterable<Long>> grouped =  rdd.groupByKey();
      
      // STEP-6: drop keys if there is a 0 in Iterable<Long>
      // otherwise keep the key and aggregate values:
      // the sum of the input values for non-direct friends
      // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
      // Pass each value in the key-value pair RDD through a map function without
      // changing the keys; this also retains the original RDD's partitioning.
      JavaPairRDD<Tuple2<Long,Long>, Long> aggregated =  grouped.mapValues(
          new Function<
                       Iterable<Long>,  // input
                       Long             // output (result of ttest)
                      >() {
          @Override
          public Long call(Iterable<Long> statusList) {
             long sum = 0l;
             for (Long status : statusList) {
                if (status == 0l) {
                   return 0l; // next we will filter out these (k,v) when v = 0
                }
                sum +=status;
            }
            return sum;
          }
      });      
      
      
      // STEP-7: next we will filter out these (k,v) when v = 0
      // If a v == 0, then exclude them
     JavaPairRDD<Tuple2<Long,Long>, Long> filtered = aggregated.filter(
        new Function<Tuple2<Tuple2<Long,Long>, Long>,Boolean>() {
        @Override
        public Boolean call(Tuple2<Tuple2<Long,Long>, Long> s) {
           if (s._2 == 0l) {
              // exclude (k,v)'s when v == 0
              return false;
           }
           else {  
              return true;
           }
        }
     });
     

     // STEP-7:generate possible friends recommendations
      // K = Tuple2<Long,Long> = Tuple2(user1,user2)
      // V = Long = 0 if diret friend, 1 if 2nd-degree friend
      JavaPairRDD<Long,Tuple2<Long,Long>> possibleRecommendations = filtered.flatMapToPair(
          new PairFlatMapFunction<Tuple2<Tuple2<Long,Long>,Long>, Long, Tuple2<Long,Long>>() {
          @Override
          public Iterator<Tuple2<Long, Tuple2<Long,Long>>> call(Tuple2<Tuple2<Long,Long>,Long> element) { 
              List<Tuple2<Long,Tuple2<Long,Long>>> results = new ArrayList<Tuple2<Long,Tuple2<Long,Long>>>();
              // identify two users with mutual number of friends
              long user1 = element._1._1;
              long user2 = element._1._2;
              long numberOfMutualFriends = element._2;
              
              // s1 is a value representing that user1 has mutualFriends in common with user2
              Tuple2<Long,Long> s1 = new Tuple2<Long,Long>(user2, numberOfMutualFriends);
              results.add(new Tuple2(user1, s1));

              // string 2 is a value representing that user2 has mutualFriends in common with user1
              Tuple2<Long,Long> s2 = new Tuple2<Long,Long>(user1, numberOfMutualFriends);
              results.add(new Tuple2(user2, s2));
              return results.iterator();
          }
      });
     
    
     // STEP-8: prepare final RDD for friends recommendations
      JavaPairRDD<Long,Iterable<Tuple2<Long,Long>>> finalRDD = possibleRecommendations.groupByKey();

      JavaPairRDD<Long, List<Long>> recommendations =  finalRDD.mapValues(
          new Function<
                       Iterable<Tuple2<Long,Long>>,  // input
                       List<Long>                    // output (recommendations)
                      >() {
          public List<Long> call(Iterable<Tuple2<Long,Long>> values) {
             // step-8.1: build sorted map of friendship
             TreeMap<Long, List<Long>> sortedMap = buildSortedMap(values);

             // step-8.2: now select the top maxNumberOfRecommendations users 
             // to recommend as potential friends
             List<Long> recommendations = getTopFriends(sortedMap, maxNumberOfRecommendations);
             return recommendations;
          }
      });        

      // STEP-9: done
      recommendations.saveAsTextFile(outputPath);
      ctx.close();
      System.exit(0);
   }
    
   static TreeMap<Long, List<Long>> buildSortedMap(Iterable<Tuple2<Long,Long>> values) {
        TreeMap<Long, List<Long>> sortedMap = new TreeMap<Long, List<Long>>();
        for (Tuple2<Long,Long> pair : values) {
           long potentialFriend = pair._1;
           long mutualFriend = pair._2;
           List<Long> list = sortedMap.get(mutualFriend);
           if (list == null) {
              // there are no users yet with this number of mutual friends
              list = new ArrayList<Long>();
              list.add(potentialFriend);
              sortedMap.put(mutualFriend, list);
           }
           else {
              // there are already users with this number of mutual friends
              list.add(potentialFriend);
           }
        }
        return sortedMap;
   }
    
    
   static List<Long> getTopFriends(TreeMap<Long, List<Long>> sortedMap, int N) {   
        // now select the top N users to recommend as potential friends
        List<Long> recommendations = new ArrayList<Long>();
        for (long mutualFriends : sortedMap.descendingKeySet()) {
            List<Long> potentialFriends = sortedMap.get(mutualFriends);
            Collections.sort(potentialFriends);
            for (long potentialFriend : potentialFriends) {
                recommendations.add(potentialFriend);
                if (recommendations.size() == N) {
                   return recommendations;
                }
            }
        }
        
        // here we have less than N friends recommendations
        return recommendations;
   }    
}
