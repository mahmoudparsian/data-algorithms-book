package org.dataalgorithms.chap11.spark;

// STEP-0: import required classes and interfaces
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
//
import java.io.Serializable;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
//
import org.apache.commons.lang.StringUtils;
//
import org.dataalgorithms.util.DateUtil;

/**
 * Input record format: 
 *   <customerID><,><transactionD><,><purchaseDate><,><amount>
 *
 * STEP-1: handle input parameters
 *
 * STEP-2: convert input into RDD<String> where each element is an input record
 *
 * STEP-3: convert RDD<String> into JavaPairRDD<K,V>, where 
 *         K: customerID
 *         V: Tuple2<purchaseDate, Amount>
 *
 * STEP-4: Group transactions by customerID: apply groupByKey() 
 *         to the output of STEP-2, result will be:
 *         JavaPairRDD<K2,V2>, where 
 *         K2: customerID
 *         V2: Iterable<Tuple2<purchaseDate, Amount>>
 *
 * STEP-5: Create Markov "state sequence": State1, State2, ..., StateN
 *         mapValues() of JavaPairRDD<K2,V2> and generate JavaPairRDD<K4, V4>
 *         First convert (K2, V2) into (K3, V3) pairs [K2 = K3 = K4]
 *         V3: sorted(V2) (order is based on purchaseDate)
 *         V3: is a sorted "transaction sequence"
 *         Then use V3 to create Markov "state sequence" (as V4)
 *
 *
 * STEP-6: Generate Markov State Transition
 *         Input is JavaPairRDD<K4, V4> pairs 
 *         Output is a matrix of states {S1, S2, S3, ...}
 *
 *            | S1   S2   S3   ...
 *         ---+-----------------------
 *         S1 |    <probability-value>
 *            |
 *         S2 |
 *            |
 *         S3 |
 *            |
 *         ...| 
 * 
 *         which defines the probability of going from one state to 
 *         another state.  After this matrix is built, we can use new 
 *         data to predict the next marketing date.
 *
 * STEP-7: emit final output
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkMarkov implements Serializable {

   static List<Tuple2<Long,Integer>> toList(Iterable<Tuple2<Long,Integer>> iterable) {
      List<Tuple2<Long,Integer>> list = new ArrayList<Tuple2<Long,Integer>>();
      for (Tuple2<Long,Integer> element: iterable) {
         list.add(element);
      }
      return list;
   }
   
    static String getElapsedTime(long daysDiff) {
        if (daysDiff < 30) {
            return "S"; // small
        } else if (daysDiff < 60) {
            return "M"; // medium
        } else {
            return "L"; // large
        }
    }  
    
    static String getAmountRange(int priorAmount, int amount) {
        if (priorAmount < 0.9 * amount) {
            return "L"; // significantly less than
        } else if (priorAmount < 1.1 * amount) {
            return "E"; // more or less same
        } else {
            return "G"; // significantly greater than
        }
    }

   /**
    * @param list : List<Tuple2<Date,Amount>>
    * list = [T2(Date1,Amount1), T2(Date2,Amount2), ..., T2(DateN,AmountN)
    * where Date1 <= Date2 <= ... <= DateN
    */
   static List<String> toStateSequence(List<Tuple2<Long,Integer>> list) {
      if (list.size() < 2) {
        // not enough data
        return null;
      }
      List<String> sequence = new ArrayList<String>();
      Tuple2<Long,Integer> prior = list.get(0);
      for (int i = 1; i < list.size(); i++) {
         Tuple2<Long,Integer> current = list.get(i);
         //
         long priorDate = prior._1;
         long date = current._1;
         // one day = 24*60*60*1000 = 86400000 milliseconds
         long daysDiff = (date - priorDate) / 86400000;

         int priorAmount = prior._2;
         int amount = current._2;
         //
         String elapsedTime = getElapsedTime(daysDiff);
         String amountRange = getAmountRange(priorAmount, amount);
         //
         String element = elapsedTime + amountRange;
         sequence.add(element);
         prior = current; 
      }
      return sequence;
   } 
     
   public static void main(String[] args) throws Exception {
   
      // STEP-1: handle input parameters
      if (args.length != 1) {
         System.err.println("Usage: SparkMarkov <input-path>");
         System.exit(1);
      }
      final String inputPath = args[0];
      System.out.println("inputPath:args[0]="+args[0]);

      // STEP-2: convert input into RDD<String> where each element is an input record
      JavaSparkContext ctx = new JavaSparkContext();
      JavaRDD<String> records = ctx.textFile(inputPath, 1);
      records.saveAsTextFile("/output/2");

      // You may optionally partition RDD
      // public JavaRDD<T> coalesce(int N)
      // Return a new RDD that is reduced into N partitions.
      // JavaRDD<String> records = ctx.textFile(inputPath, 1).coalesce(9);

      // STEP-3: convert RDD<String> into JavaPairRDD<K,V>, where 
      //    K: customerID
      //    V: Tuple2<purchaseDate, Amount> : Tuple2<Long, Integer>
      //    PairFunction<T, K, V>
      //    T => Tuple2<K, V>
      JavaPairRDD<String, Tuple2<Long,Integer>> kv = records.mapToPair(
         new PairFunction<
                          String,               // T
                          String,               // K
                          Tuple2<Long,Integer>  // V
                          >() {
         @Override
         public Tuple2<String,Tuple2<Long,Integer>> call(String rec) {
             String[] tokens = StringUtils.split(rec, ",");
             if (tokens.length != 4) {
                // not a proper format
                return null;
             }
             // tokens[0] = customer-id
             // tokens[1] = transaction-id
             // tokens[2] = purchase-date
             // tokens[3] = amount
             long date = 0;
             try {
                 date = DateUtil.getDateAsMilliSeconds(tokens[2]);
             }
             catch(Exception e) {
                // ignore for now -- must be handled 
             }
             int amount = Integer.parseInt(tokens[3]);
             Tuple2<Long,Integer> V = new Tuple2<Long,Integer>(date, amount);
             return new Tuple2<String,Tuple2<Long,Integer>>(tokens[0], V);
         }
      });
      kv.saveAsTextFile("/output/3");

      // STEP-4: Group transactions by customerID: apply groupByKey() 
      //         to the output of STEP-2, result will be:
      //         JavaPairRDD<K2,V2>, where 
      //           K2: customerID
      //           V2: Iterable<Tuple2<purchaseDate, Amount>>
      JavaPairRDD<String, Iterable<Tuple2<Long,Integer>>> customerRDD = kv.groupByKey();
      customerRDD.saveAsTextFile("/output/4");

      // STEP-5: Create Markov "state sequence": State1, State2, ..., StateN
      //         mapValues() of JavaPairRDD<K2,V2> and generate JavaPairRDD<K4, V4>
      //         First convert (K2, V2) into (K3, V3) pairs [K2 = K3 = K4]
      //         V3: sorted(V2) (order is based on purchaseDate)
      //         V3: is a sorted "transaction sequence"
      //         Then use V3 to create Markov "state sequence" (as V4)    
      // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
      // Pass each value in the key-value pair RDD through a map function without
      // changing the keys; this also retains the original RDD's partitioning.
      JavaPairRDD<String, List<String>> stateSequence =  customerRDD.mapValues(
          new Function<
                       Iterable<Tuple2<Long,Integer>>,  // input
                       List<String>                     // output ("state sequence")
                      >() {
          @Override
          public List<String> call(Iterable<Tuple2<Long,Integer>> dateAndAmount) {
             List<Tuple2<Long,Integer>> list = toList(dateAndAmount);
             Collections.sort(list, TupleComparatorAscending.INSTANCE);
             // now convert sorted list (be date) into a "state sequence"
             List<String> stateSequence = toStateSequence(list);
             return stateSequence;
          }
      }); 
      stateSequence.saveAsTextFile("/output/5");

      // STEP-6: Generate Markov State Transition
      //          Input is JavaPairRDD<K4, V4> pairs 
      //            where K4=customerID, V4 = List<State>
      //          Output is a matrix of states {S1, S2, S3, ...}
      // 
      //             | S1   S2   S3   ...
      //          ---+-----------------------
      //          S1 |    <probability-value>
      //             |
      //          S2 |
      //             |
      //          S3 |
      //             |
      //          ...| 
      //  
      //          which defines the probability of going from one state to 
      //          another state.  After this matrix is built, we can use new 
      //          data to predict the next marketing date.
      // For implementation of this step, we use:
      //     PairFlatMapFunction<T, K, V>
      //     T => Iterable<Tuple2<K, V>>
      JavaPairRDD<Tuple2<String,String>, Integer> model = stateSequence.flatMapToPair(
         new PairFlatMapFunction<
                                 Tuple2<String, List<String>>,  // T
                                 Tuple2<String,String>,         // K
                                 Integer                        // V
                                >() {
         @Override
         public Iterator<Tuple2<Tuple2<String,String>, Integer>> call(Tuple2<String, List<String>> s) {
            List<String> states = s._2;
            if ( (states == null) || (states.size() < 2) ) {
               return Collections.EMPTY_LIST.iterator();
            }
         
            List<Tuple2<Tuple2<String,String>, Integer>> mapperOutput =
                new ArrayList<Tuple2<Tuple2<String,String>, Integer>>();
            for (int i = 0; i < (states.size() -1); i++) {
                String fromState = states.get(i);
                String toState = states.get(i+1);
                Tuple2<String,String> k = new Tuple2<String,String>(fromState, toState);
                mapperOutput.add(new Tuple2<Tuple2<String,String>, Integer>(k, 1));
            }
            return mapperOutput.iterator();
         }
       });
       model.saveAsTextFile("/output/6.1");
    
       // combine/reduce frequent (fromState, toState)
       JavaPairRDD<Tuple2<String,String>, Integer> markovModel = 
           model.reduceByKey(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer i1, Integer i2) {
              return i1 + i2;
           }
       });
       markovModel.saveAsTextFile("/output/6.2");    

       // STEP-7: emit final output
       // convert markovModel into "<fromState><,><toState><TAB><count>"
       // Use map() to convert JavaPairRDD into JavaRDD:
       // <R> JavaRDD<R> map(Function<T,R> f)
       // Return a new RDD by applying a function to all elements of this RDD.
       JavaRDD<String> markovModelFormatted = 
          markovModel.map(new Function<Tuple2<Tuple2<String,String>, Integer>, String>() {
          @Override
          public String call(Tuple2<Tuple2<String,String>, Integer> t) {
            return t._1._1 + "," + t._1._2 + "\t" + t._2;
          }
       });
       markovModelFormatted.saveAsTextFile("/output/6.3");
       
       // done
       ctx.close();
       System.exit(0);
   }

   static class TupleComparatorAscending implements Comparator<Tuple2<Long, Integer>>, Serializable {
       final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();
       @Override
       public int compare(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) {
          // return -t1._1.compareTo(t2._1);     // sorts RDD elements descending
          return t1._1.compareTo(t2._1);         // sorts RDD elements ascending
       }
   }
      
}
