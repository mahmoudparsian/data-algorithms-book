package org.dataalgorithms.chap13.spark;


// STEP-0: Import required classes and interfaces
import java.util.Map;
import java.util.SortedMap;
//
import scala.Tuple2;
//
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
//
import org.dataalgorithms.chap13.util.Util;

/**
 * This class solves K-Nearest-Neighbor join operation using Spark API.
 *
 * @author Mahmoud Parsian
 *
 */
public class kNN {
   
   public static void main(String[] args) throws Exception {
    // STEP-1: Handle input parameters
    if (args.length < 5) {
      System.err.println("Usage: kNN <k-knn> <d-dimension> <R> <S> <output-path>");
      System.exit(1);
    }
    //
    Integer k = Integer.valueOf(args[0]); // k for kNN
    Integer d = Integer.valueOf(args[1]); // d-dimension
    String datasetR = args[2];
    String datasetS = args[3];
    String outputPath = args[4];
    
    // STEP-2: Create a Spark session object
    SparkSession session = SparkSession
      .builder()
      .appName("knn")
      .getOrCreate();

    JavaSparkContext context = JavaSparkContext.fromSparkContext(session.sparkContext());
    
    // STEP-3: Broadcast shared objects
    // broadcast k and d as global shared objects,
    // which can be accessed from all cluster nodes
    final Broadcast<Integer> broadcastK = context.broadcast(k);
    final Broadcast<Integer> broadcastD = context.broadcast(d);

    // STEP-4: Create RDDs for query and training datasets
    JavaRDD<String> R = session.read().textFile(datasetR).javaRDD();
    R.saveAsTextFile(outputPath+"/R");  
    JavaRDD<String> S = session.read().textFile(datasetS).javaRDD();
    S.saveAsTextFile(outputPath+"/S");

    // STEP-5: Perform cartesian product of (R, S)
    //<U> JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)
    // Return the Cartesian product of this RDD and another 
    // one, that is, the RDD of all pairs of elements (a, b) 
    // where a is in this and b is in other.
    JavaPairRDD<String,String> cart = R.cartesian(S);
    cart.saveAsTextFile(outputPath+"/cart");
    
    // STEP-6: Find distance(r, s) for r in R and s in S
    // (K,V), where K = unique-record-id-of-R, V=Tuple2(distance, classification)
    // distance = distance(r, s) where r in R and s in S
    // classification is extracted from s 
    JavaPairRDD<String,Tuple2<Double,String>> knnMapped =
            //                              input                  K       V
            cart.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<Double,String>>() {
      @Override
      public Tuple2<String,Tuple2<Double,String>> call(Tuple2<String,String> cartRecord) {
        String rRecord = cartRecord._1;
        String sRecord = cartRecord._2;
        String[] rTokens = rRecord.split(";"); 
        String rRecordID = rTokens[0];
        String r = rTokens[1]; //  r.1, r.2, ..., r.d
        String[] sTokens = sRecord.split(";"); 
        // sTokens[0] = s.recordID
        String sClassificationID = sTokens[1]; 
        String s = sTokens[2]; // s.1, s.2, ..., s.d
        Integer d = broadcastD.value();
        double distance = Util.calculateDistance(r, s, d);
        String K = rRecordID; //  r.recordID
        Tuple2<Double,String> V = new Tuple2<Double,String>(distance, sClassificationID);
        return new Tuple2<String,Tuple2<Double,String>>(K, V);
      }
    });
    knnMapped.saveAsTextFile(outputPath+"/knnMapped");    

    // STEP-7: Group distances by r in R
    // now group the results by r.recordID and then find the k-nearest-neigbors.
    JavaPairRDD<String, Iterable<Tuple2<Double,String>>> knnGrouped = knnMapped.groupByKey();
    
    // STEP-8: find the k-nearest-neigbors and classify r
    // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
    // Pass each value in the key-value pair RDD through a 
    // map function without changing the keys;
    // this also retains the original RDD's partitioning.
    // Generate (K,V) pairs where K=r.recordID, V = classificationID
    JavaPairRDD<String, String> knnOutput =
        knnGrouped.mapValues(new Function<Iterable<Tuple2<Double,String>>, // input
                                          String                           // output (classification)
                                      >() {
      @Override
      public String call(Iterable<Tuple2<Double,String>> neighbors) {
          Integer k = broadcastK.value();
          // keep only k-nearest-neighbors
          SortedMap<Double, String> nearestK = Util.findNearestK(neighbors, k);
          
          // now we have the k-nearest-neighbors in nearestK
          // we need to find out the classification by majority
          // count classifications
          Map<String, Integer> majority = Util.buildClassificationCount(nearestK);
            
          // find a classificationID with majority of vote
          String selectedClassification = Util.classifyByMajority(majority);
          return selectedClassification;
      }
    }); 
    //
    knnOutput.saveAsTextFile(outputPath+"/knnOutput");

    // done 
    session.stop();
    System.exit(0);
  }
}
