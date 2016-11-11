package org.dataalgorithms.chap28.spark;

// STEP-0: import required classes and interfaces
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

/**
 * Given {(K,V)}, the goal is to find mean of values for a given K.
 * We will create structures in such a way that if combiner is used,
 * then the "mean" of "mean" will correctly return the mean of all 
 * values. For this example, we create monoids so that combiners 
 * can be utilized without losing the semantics of "mean" function.
 * 
 * In Spark, calling reduceByKey() will automatically perform 
 * combining locally on each machine before computing global totals 
 * for each key. The programmer does not need to specify a combiner. 
 * Since combining is automatic, we need to pay an extra attention to
 * reduceByKey(), to make sure that using combiners will not alter 
 * the semantics of our desired function (in this example, the desired 
 * functionality is the "mean" function). Below, we provide a solution
 * to "mean" function by providing monoids structures so that the correct 
 * semantics of "mean" function is preserved. The entire solution is 
 * presented as a single Java class.
 * 
 * Note that "mean" of "mean" is not a monoid. Therefore, to preserve
 * the semantics of "mean" over a set of long data type numbers, we 
 * have to provide a monoid structure so that combiners can be used 
 * efficiently and correctly.
 * 
 * To find means of numbers, convert each number into (number, 1),
 * then add them preserving a monoid structure:
 * 
 * The monoid structure is defined as (sum, count)
 * 
 * number1 -> (number1, 1)
 * number2 -> (number2, 1)
 * (number1, 1) + (number2, 1) -> (number1+number2, 1+1) = (number1+number2, 2)
 * (number1, x) + (number2, y) ->  (number1+number2, x+y)
 * 
 * Finally, per key, we will have a value as (sum, count), then to find the mean,
 * mean = sum / count
 * 
 *
 * 
 * @author Mahmoud Parsian
 *
 */
public class SparkMeanMonoidized  {

   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length != 1) {
         System.err.println("Usage: SparkMeanMonodized <input-path>");
         System.exit(1);
      }
      final String inputPath = args[0];

      // STEP-2: create an RDD from input
      //    input record format:
      //        <string-key><TAB><long-value>
      JavaSparkContext ctx = new JavaSparkContext();
      JavaRDD<String> records = ctx.textFile(inputPath, 1);
      records.saveAsTextFile("/output/2");

      // STEP-3: create a monoid
      // map input(T) into (K,V) pair, which is monodic
      JavaPairRDD<String,Tuple2<Long,Integer>> monoid = 
         records.mapToPair(new PairFunction<String,String,Tuple2<Long,Integer>>() {
         @Override
         public Tuple2<String,Tuple2<Long,Integer>> call(String s) {
            String[] tokens = s.split("\t"); //  <key><TAB><value>
            String K = tokens[0];
            Tuple2<Long,Integer> V = new Tuple2<Long,Integer>(Long.parseLong(tokens[1]), 1);
            return new Tuple2<String,Tuple2<Long,Integer>>(K, V);
         }
      });
      monoid.saveAsTextFile("/output/3");

      // STEP-4: reduce frequent K's with preserving monoids
      // Combiners may be used without losing the semantics of "mean"
      JavaPairRDD<String, Tuple2<Long,Integer>> reduced = monoid.reduceByKey(
         new Function2<Tuple2<Long,Integer>, Tuple2<Long,Integer>, Tuple2<Long,Integer>>() {
         @Override
         public Tuple2<Long,Integer> call(Tuple2<Long,Integer> v1, Tuple2<Long,Integer> v2) {
            return new Tuple2<Long,Integer>(v1._1+ v2._1, v1._2+ v2._2);
         }
      });
      reduced.saveAsTextFile("/output/4");
      // now reduced RDD has the desired values for final output     

      // STEP-5: find mean by mapping values
      // mapValues[U](f: (V) ⇒ U): JavaPairRDD[K, U]
      // Pass each value in the key-value pair RDD through 
      // a map function without changing the keys; 
      // this also retains the original RDD's partitioning.      
      JavaPairRDD<String,Double> mean = reduced.mapValues(
         new Function<
                       Tuple2<Long,Integer>,  // input
                       Double                 // output
                      >() {  
         @Override
         public Double call(Tuple2<Long, Integer> s) {
           return ( (double) s._1 / (double) s._2 );
         }
      });
      mean.saveAsTextFile("/output/5");
      System.exit(0);
   }
}
