package org.dataalgorithms.chap07.spark;


// STEP-0: import required classes and interfaces

import java.util.List;
import java.util.ArrayList;
//
import scala.Tuple2;
import scala.Tuple3;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//
import org.dataalgorithms.util.Combination;

/**
 * 
 * The FindAssociationRules class finds all association rules 
 * for a market basket data sets.
 *  
 * @author Mahmoud Parsian
 *
 */ 
public class FindAssociationRulesWithLambda {
  
   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length < 1) {
         System.err.println("Usage: FindAssociationRulesWithLambda <transactions>");
         System.exit(1);
      }
      String transactionsFileName =  args[0];

      // STEP-2: create a Spark context object
      JavaSparkContext ctx = new JavaSparkContext();
       
      // STEP-3: read all transactions from HDFS and create the first RDD 
      JavaRDD<String> transactions = ctx.textFile(transactionsFileName, 1);
      transactions.saveAsTextFile("/rules/output/1");

      // STEP-4: generate frequent patterns
      // PairFlatMapFunction<T, K, V>     
      // T => Iterable<Tuple2<K, V>>
      JavaPairRDD<List<String>,Integer> patterns = 
         transactions.flatMapToPair((String transaction) -> {
             List<String> list = Util.toList(transaction);
             List<List<String>> combinations = Combination.findSortedCombinations(list);
             List<Tuple2<List<String>,Integer>> result = new ArrayList<Tuple2<List<String>,Integer>>();
             for (List<String> combList : combinations) {
                 if (combList.size() > 0) {
                     result.add(new Tuple2<List<String>,Integer>(combList, 1));
                 }
             }
             return result.iterator();
      });
      //
      patterns.saveAsTextFile("/rules/output/2");
    
      // STEP-5: combine/reduce frequent patterns
      JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey((Integer i1, Integer i2) -> i1 + i2);  
      //
      combined.saveAsTextFile("/rules/output/3");
    
      // now, we have: patterns(K,V)
      //      K = pattern as List<String>
      //      V = frequency of pattern
      // now given (K,V) as (List<a,b,c>, 2) we will 
      // generate the following (K2,V2) pairs:
      //
      //   (List<a,b,c>, T2(null, 2))
      //   (List<a,b>,   T2(List<a,b,c>, 2))
      //   (List<a,c>,   T2(List<a,b,c>, 2))
      //   (List<b,c>,   T2(List<a,b,c>, 2))


      // STEP-6: generate all sub-patterns
      // PairFlatMapFunction<T, K, V>     
      // T => Iterable<Tuple2<K, V>>
      JavaPairRDD<List<String>,Tuple2<List<String>,Integer>> subpatterns = 
         combined.flatMapToPair((Tuple2<List<String>, Integer> pattern) -> {
             List<Tuple2<List<String>,Tuple2<List<String>,Integer>>> result =
                     new ArrayList<Tuple2<List<String>,Tuple2<List<String>,Integer>>>();
             List<String> list = pattern._1;
             Integer frequency = pattern._2;
             result.add(new Tuple2(list, new Tuple2(null,frequency)));
             if (list.size() == 1) {
                 return result.iterator();
             }
             
             // pattern has more than one items
             // result.add(new Tuple2(list, new Tuple2(null,size)));
             for (int i=0; i < list.size(); i++) {
                 List<String> sublist = Util.removeOneItem(list, i);
                 result.add(new Tuple2(sublist, new Tuple2(list, frequency)));
             }
             return result.iterator();
      });
      //
      subpatterns.saveAsTextFile("/rules/output/4");
        
      // STEP-6: combine sub-patterns
      JavaPairRDD<List<String>,Iterable<Tuple2<List<String>,Integer>>> rules = subpatterns.groupByKey();       
      rules.saveAsTextFile("/rules/output/5");

      // STEP-7: generate association rules      
      // Now, use (K=List<String>, V=Iterable<Tuple2<List<String>,Integer>>) 
      // to generate association rules
      // JavaRDD<R> map(Function<T,R> f)
      // Return a new RDD by applying a function to all elements of this RDD.
      // T: input      
      // R: ( ac => b, 1/3): T3(List(a,c), List(b),  0.33)
      //    ( ad => c, 1/3): T3(List(a,d), List(c),  0.33)
      JavaRDD<List<Tuple3<List<String>,List<String>,Double>>> assocRules = 
              rules.map((Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>> in) -> {
          List<Tuple3<List<String>,List<String>,Double>> result =
                  new ArrayList<Tuple3<List<String>,List<String>,Double>>();
          List<String> fromList = in._1;
          Iterable<Tuple2<List<String>,Integer>> to = in._2;
          List<Tuple2<List<String>,Integer>> toList = new ArrayList<Tuple2<List<String>,Integer>>();
          Tuple2<List<String>,Integer> fromCount = null;
          for (Tuple2<List<String>,Integer> t2 : to) {
              // find the "count" object
              if (t2._1 == null) {
                  fromCount = t2;
              }
              else {
                  toList.add(t2);
              }
          }
          
          // Now, we have the required objects for generating association rules:
          //  "fromList", "fromCount", and "toList"
          if (toList.isEmpty()) {
              // no output generated, but since Spark does not like null objects, we will fake a null object
              return result; // an empty list
          }
          
          // now using 3 objects: "from", "fromCount", and "toList",
          // create association rules:
          for (Tuple2<List<String>,Integer>  t2 : toList) {
              double confidence = (double) t2._2 / (double) fromCount._2;
              List<String> t2List = new ArrayList<String>(t2._1);
              t2List.removeAll(fromList);
              result.add(new Tuple3(fromList, t2List, confidence));
          }
          return result;
      });   
      assocRules.saveAsTextFile("/rules/output/6");

      // done
      ctx.close();  
      
      System.exit(0);
   }
}

