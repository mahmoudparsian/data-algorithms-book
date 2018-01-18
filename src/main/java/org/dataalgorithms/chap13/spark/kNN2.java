package org.dataalgorithms.chap13.spark;


// STEP-0: Import required classes and interfaces
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
//
import org.apache.commons.lang3.StringUtils;
//
import scala.Tuple2;
//
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
//
import org.dataalgorithms.chap13.util.Util;

/**
 * This class solves K-Nearest-Neighbor join operation using Spark API.
 * 
 * R: query data, may be a much smaller set than S.size()
 *   <id><,><feature-1><,><feature-2><,>...<feature-d>
 * 
 * S: training data, which is classified, can be huge in millions of records
 *   <classificationID><,><feature-1><,><feature-2><,>...<feature-d>
 * 
 * To do the join(R,S), we will map S and for each map transformation, we
 * will use all of R's data: this way we accomplish the join operation
 * 
 * Expected output:
 *   <id><,><classificationID>
 *
 * @author Mahmoud Parsian
 *
 */
public class kNN2 {
   
   public static void main(String[] args) throws Exception {
    // STEP-1: Handle input parameters
    if (args.length < 5) {
      System.err.println("Usage: kNN2 <k-knn> <d-dimension> <R> <S> <output-path>");
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
      .appName("knn2")
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
    
    // JavaPairRDD<String,String> cart = R.cartesian(S);
    // cart.saveAsTextFile(outputPath+"/cart");
    //
    // rather than doing cartesian(R, S), I will put R into a 
    // Map<id, {<feature-1><,><feature-2><,>...<feature-d>}>
    // and then use this hash Map object for every record of S 
    // (this will accomplish the cartesian product)
    //
    final Map<String, double[]> rMap = convertToMap(R);
    
    // STEP-6: Find distance(r, s) for r in R and s in S
    // (K,V), where K = unique-record-id-of-R, V=Tuple2(distance, classification)
    // distance = distance(r, s) where r in R and s in S
    // classification is extracted from s 

    //flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {   
    JavaPairRDD<String,Tuple2<Double,String>> knnMapped =
            S.flatMapToPair(new PairFlatMapFunction<
                                                    String,                // s.record
                                                    String,                // K = unique-record-id-of-R
                                                    Tuple2<Double,String>  // V = Tuple2(distance, classification)
                                                   >() {
      @Override
      public Iterator<Tuple2<String,Tuple2<Double,String>>> call(String sRecord) {
        Integer d = broadcastD.value();        
        //
        // prepare S.features
        //
        String[] sTokens = sRecord.split(","); 
        String sClassificationID = sTokens[0]; 
        double[] sFeatures = new double[d];
        for (int i=1; i < sTokens.length; i++){
            sFeatures[i-1] = Double.parseDouble(sTokens[i]); // s.1, s.2, ..., s.d
        }
        
        List<Tuple2<String,Tuple2<Double,String>>> result = new ArrayList<>();
        //
        // iterate R and create a join of (R and S)
        //
        for (Map.Entry<String, double[]> rEntry : rMap.entrySet()) {
            String key = rEntry.getKey(); // K = unique-record-id-of-R
            double[] rFeatures = rEntry.getValue();
            double distance = Util.calculateDistance(rFeatures, sFeatures, d);            
            //
            result.add(new Tuple2<>(key, new Tuple2<>(distance, sClassificationID)));
        }
        //
        return result.iterator();
      }
    });
    knnMapped.saveAsTextFile(outputPath+"/knnMapped");    

    // STEP-7: Group distances by r in R
    // now group the results by r.recordID and then find the k-nearest-neighbors.
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
   
  static Map<String, double[]>  convertToMap(JavaRDD<String> R) {
      Map<String, double[]> map = new HashMap<>();
      List<String> list = R.collect();
      for (String r : list) {
          String[] tokens = StringUtils.split(r, ",");
          // id = tokens[0]
          // feature-1 = tokens[1]
          // feature-2 = tokens[2]
          // ...
          double[] features = new double[tokens.length -1];
          for (int i = 1; i < tokens.length; i++) {
              features[i-1] = Double.parseDouble(tokens[i]);
          }
          //
          map.put(tokens[0], features);          
      }
      //
      return map;
  }
   
   
}
