package org.dataalgorithms.machinelearning.kmeans;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.lang.StringUtils;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.log4j.Logger;

/**
 * This example is adapted and revised from: AMP Camp Big Data Mini Course:
 *     http://www.cs.berkeley.edu/~rxin/ampcamp-ecnu/featurization.html
 *     http://www.cs.berkeley.edu/~rxin/ampcamp-ecnu/machine-learning-with-spark.html
 * 
 * K-Means clustering is a clustering algorithm that can be used to 
 * partition your dataset into K clusters. We implement K-Means clustering 
 * using Spark to cluster the featurized Wikipedia dataset.
 * 
 * Command Line Preprocessing and Featurization: to apply most machine 
 * learning algorithms, we must first preprocess and featurize the data. 
 * For featureiztion, please see the java class:
 *      org.dataalgorithms.machinelearning.kmeans.Featurization

 * That is, for each data point, we must generate a vector of numbers 
 * describing the salient properties of that data point. In our case, 
 * each data point will consist of a unique Wikipedia article identifier 
 * (i.e., a unique combination of Wikipedia project code and page title) 
 * and associated traffic statistics. We will generate 24-dimensional 
 * feature vectors (one feature per hour), with each feature vector 
 * entry summarizing the page view counts for the corresponding hour 
 * of the day.
 * 
 *  Each input record (before featureization of data) in our dataset 
 *  consists of a string with the format:
 *  “<date_time> <project_code> <page_title> <num_hits> <page_size>”. 
 *  Note that the format of the "<date-time>" field is YYYYMMDD-HHmmSS
 *  (where ‘M’ denotes month, and ‘m’ denotes minute).
 * 
 * The first few lines of the file are copied here:
 *
 *   20090507-040000 aa ?page=http://www.stockphotosharing.com/Themes/Images/users_raw/id.txt 3 39267
 *   20090507-040000 aa Main_Page 7 51309
 *   20090507-040000 aa Special:Boardvote 1 11631
 *   20090507-040000 aa Special:Imagelist 1 931
 *   20090505-000000 aa.b ?71G4Bo1cAdWyg 1 14463
 *   20090505-000000 aa.b Special:Statistics 1 840
 *   20090505-000000 aa.b Special:Whatlinkshere/MediaWiki:Returnto 1 1019
 *   20090505-000000 aa.b Wikibooks:About 1 15719
 *   20090505-000000 aa ?14mFX1ildVnBc 1 13205
 *   20090505-000000 aa ?53A%2FuYP3FfnKM 1 13207
 *   20090505-000000 aa ?93HqrnFc%2EiqRU 1 13199
 *   20090505-000000 aa ?95iZ%2Fjuimv31g 1 13201
 *   20090505-000000 aa File:Wikinews-logo.svg 1 8357
 *   20090505-000000 aa Main_Page 2 9980
 *
 * Data source download: https://aws.amazon.com/datasets/wikipedia-traffic-statistics-v2/
 * 
 * Steps to preprocess and featurize the Wikipedia dataset:
 *    http://www.cs.berkeley.edu/~rxin/ampcamp-ecnu/featurization.html
 * 
 * After we featurize data (by using the Featurization class), our actual input records 
 * for K-Means will input records will be (which will be fed to the K-Means algorithm)
 * 
 *  <key><#><feature_1><,><feature_2><,>...<,><feature_24>
 * 
 *   where 
 *      key as a String: <project_code> + " " + <page_title>
 *      and <feature_1>, <feature_2>, ...<feature_24> are double data types
 * 
 * 
 *  Challenge Exercise: The K-Means implementation uses a groupBy and mapValues 
 *  to compute the new centers. This can be optimized by using a running sum of 
 *  the vectors that belong to a cluster and running counter of the number of 
 *  vectors present in a cluster. How would you use the Spark API to implement this?
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
 public class WikipediaKMeansUsingMLlibVector {
     
    private static final Logger THE_LOGGER = Logger.getLogger(WikipediaKMeansUsingMLlibVector.class);
     
    
    static double squaredDistance(Vector a, Vector b) {
        double  distance = 0.0;
        int size = a.size();
        for (int i=0; i < size; i++){
            double diff = a.apply(i) - b.apply(i);
            distance += diff * diff;
        }
        return distance;
    } 
    
     
    /**
     * Build a Vector from given features denoted by a comma separated feature values
     * @param features set of features in the format: 
     *   <feature_1><,><feature_2><,>...<,><feature_N>
     * @return a Vector of features
     */
    static Vector buildVector(String features) {
        String[] tokens = StringUtils.split(features, ",");
        double[] d = new double[tokens.length];
        for (int i=0; i < d.length; i++){
            d[i] = Double.parseDouble(tokens[i]);
        }
        return new DenseVector(d);
    } 
    
     
     static int closestPoint(Vector p, List<Vector> centers) {
         int bestIndex = 0;
         double closest = Double.POSITIVE_INFINITY;
         for (int i = 0; i < centers.size(); i++) {
             double tempDist = squaredDistance(p, centers.get(i));
             if (tempDist < closest) {
                 closest = tempDist;
                 bestIndex = i;
             }
         }
         return bestIndex;
     }
   
    static Vector average(List<Vector> list) {
         // find sum
         double[] sum = new double[list.get(0).size()];
         for (Vector v : list) {
             for (int i = 0; i < sum.length; i++) {
                 sum[i] += v.apply(i);
             }
         }

         // find averages...
         int numOfVectors = list.size();
         for (int i = 0; i < sum.length; i++) {
             sum[i] = sum[i] / numOfVectors;
         }
         return new DenseVector(sum);
    }
   
    static Vector average(Iterable<Vector> ps) {
         List<Vector> list = new ArrayList<Vector>();
         for (Vector v : ps) {
             list.add(v);
         }
         return average(list);
    }
   
   /**
    * Cache the result RDD, since K-Means is an iterative machine learning algorithm 
    * and the result will be used many times
    * 
    * @param wikiData, a featureized data 
    * @param context a Java spark context object 
    * @return JavaPairRDD<String, Vector>, where K is <project_code> + " " + <page_title> 
    * and V is a Vector of features
    * 
    */
   static JavaPairRDD<String, Vector> getFeatureizedData(String wikiData, JavaSparkContext context) {
      JavaPairRDD<String, Vector> data = context.textFile(wikiData).mapToPair(
       new PairFunction<String, String, Vector>() {
         @Override
         public Tuple2<String, Vector> call(String in) throws Exception {
           // in: <key><#><feature_1><,><feature_2><,>...<,><feature_24>
           String[] parts = StringUtils.split(in, "#");
           return new Tuple2<String, Vector>(parts[0], buildVector(parts[1]));
         }
       }).cache();    
      return data;
   }
   
   static Map<Integer, Vector> getNewCentroids(JavaPairRDD<Integer, Iterable<Vector>> pointsGroup) {
         Map<Integer, Vector> newCentroids = pointsGroup.mapValues(
                 new Function<Iterable<Vector>, Vector>() {
                     @Override
                     public Vector call(Iterable<Vector> ps) throws Exception {
                         return average(ps);
                     }
                 }).collectAsMap();
         return newCentroids;
    }  
   
    static JavaPairRDD<Integer, Vector> getClosest(JavaPairRDD<String, Vector> data, final List<Vector> centroids) {
         JavaPairRDD<Integer, Vector> closest = data.mapToPair(
         new PairFunction<Tuple2<String, Vector>, Integer, Vector>() {
             @Override
             public Tuple2<Integer, Vector> call(Tuple2<String, Vector> in) throws Exception {
                return new Tuple2<Integer, Vector>(closestPoint(in._2(), centroids), in._2());
             }
         }
       );  
       return closest;
    }
    
    static List<Vector> getInitialCentroids(JavaPairRDD<String, Vector> data, final int K){
        List<Tuple2<String, Vector>> centroidTuples = data.takeSample(false, K, 42);
        final List<Vector> centroids = new ArrayList<Vector>();
        for (Tuple2<String, Vector> t: centroidTuples) {
            centroids.add(t._2());
        }    
        return centroids;
    }
    
    static double getDistance(final List<Vector> centroids, final Map<Integer, Vector> newCentroids, final int K) {
       double distance = 0.0;
       for (int i = 0; i < K; i++) {
         distance += squaredDistance(centroids.get(i), newCentroids.get(i));
       } 
       return distance;
    }
   
   public static void main(String[] args) throws Exception {
       
     // Logger.getLogger("spark").setLevel(Level.WARN);
     
     // create a JavaSparkContext, which is used to create RDDs
     JavaSparkContext context = new JavaSparkContext();
     //
     final int K = 10;
     final double convergeDist = .000001;
     final String wikiData = "/dev/ampcamp/imdb_data/wikistats_featurized";
     //
     JavaPairRDD<String, Vector> data = getFeatureizedData(wikiData, context);
     THE_LOGGER.info("Number of data records " + data.count());
     //
     final List<Vector> centroids =  getInitialCentroids(data, K);
     THE_LOGGER.info("Done selecting initial centroids: "+centroids.size());
     //
     double tempDist = 1.0 + convergeDist; 
     // make sure that for the first time (tempDist > convergeDist).
     while (tempDist > convergeDist) {
       JavaPairRDD<Integer, Vector> closest = getClosest(data, centroids);
       //
       JavaPairRDD<Integer, Iterable<Vector>> pointsGroup = closest.groupByKey();
       Map<Integer, Vector> newCentroids = getNewCentroids(pointsGroup);
       //
       tempDist = getDistance(centroids, newCentroids, K);
       //
       for (Map.Entry<Integer, Vector> t: newCentroids.entrySet()) {
         centroids.set(t.getKey(), t.getValue());
       }
       THE_LOGGER.info("Finished iteration (delta = " + tempDist + ")");
     } // end-while
     //
     //
     //
     THE_LOGGER.info("Cluster with some articles:");
     int numArticles = 10;
     for (int i = 0; i < centroids.size(); i++) {
       final int index = i;
       List<Tuple2<String, Vector>> samples = data.filter(new Function<Tuple2<String, Vector>, Boolean>() {
         @Override
         public Boolean call(Tuple2<String, Vector> in) throws Exception {
            return closestPoint(in._2(), centroids) == index;
         }
       }).take(numArticles);
       //
       for(Tuple2<String, Vector> sample: samples) {
          THE_LOGGER.info(sample._1());
       }
       THE_LOGGER.info("");
     }
     
     // done
     context.stop();
     System.exit(0);
   }
   
 }