package org.dataalgorithms.chapB06.outlierdetection.spark;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.io.Serializable;
//
import scala.Tuple2;
//
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//
import org.apache.log4j.Logger;
//
import org.apache.commons.lang.StringUtils;


/**
 * Outlier Detection for Categorical Datasets
 *
 * Based on Paper: 
 *       Title: Fast Parallel Outlier Detection for Categorical Datasets using MapReduce
 *       URL: http://www.eecs.ucf.edu/georgiopoulos/sites/default/files/247.pdf
 *
 * Additional reference: 
 *       Title: Scalable and Efficient Outlier Detection in Large Distributed Data Sets with Mixed-Type Attributes
 *       URL: http://etd.fcla.edu/CF/CFE0002734/Koufakou_Anna_200908_PhD.pdf
 *
 * 
 * Sample input URL: https://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/breast-cancer-wisconsin.data
 * Sample input Data:
 *     1000025,5,1,1,1,2,1,3,1,1,2
 *     1002945,5,4,4,5,7,10,3,2,1,2
 *     1015425,3,1,1,1,2,2,3,1,1,2
 *     1016277,6,8,8,1,3,4,3,7,1,2
 *     1017023,4,1,1,3,2,1,3,1,1,2
 *     1017122,8,10,10,8,7,10,9,7,1,4
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class OutlierDetectionWithLambda {

    private static final Logger THE_LOGGER = Logger.getLogger(OutlierDetectionWithLambda.class);
      
    
    static class TupleComparatorAscending 
       implements Comparator<Tuple2<String,Double>>, Serializable {
       final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();
       // sort descending based on the double value
       @Override
       public int compare(Tuple2<String,Double> t1, 
                          Tuple2<String,Double> t2) {
          return (t1._2.compareTo(t2._2)); // sort based on AVF Score
       }
    }
    
    static class TupleComparatorDescending 
       implements Comparator<Tuple2<String,Double>>, Serializable {
       final static TupleComparatorDescending INSTANCE = new TupleComparatorDescending();
       // sort descending based on the double value
       @Override
       public int compare(Tuple2<String,Double> t1, 
                          Tuple2<String,Double> t2) {
          return -(t1._2.compareTo(t2._2)); // sort based on AVF Score
       }
    }
       
    
    
    /**
     * Add parameters for efficiency
     * 
     * https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
     *   val conf = new SparkConf()
     *   // Use a FAST serializer 
     *   .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     *   // Now it's 32 Mb of buffer by default instead of 0.064 Mb
     *   .set("spark.kryoserializer.buffer.mb","32") 
     * 
     * @return an instance of JavaSparkContext
     */
    static JavaSparkContext createJavaSparkContext() {
        JavaSparkContext context = new JavaSparkContext();
        
        //
        // inject efficiency
        //
        SparkConf sparkConf = context.getConf();
                
        // Now it's 32 Mb of buffer by default instead of 0.064 Mb
        sparkConf.set("spark.kryoserializer.buffer.mb","32");
        
        // http://www.trongkhoanguyen.com/2015/04/understand-shuffle-component-in-spark.html
        //spark.shuffle.file.buffer	32k	Size of the in-memory buffer for each 
        //                                      shuffle file output stream. These buffers 
        //                                      reduce the number of disk seeks and system 
        //                                      calls made in creating intermediate shuffle files.
        //
        sparkConf.set("spark.shuffle.file.buffer.kb","64");     

        // set a fast serializer
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // This setting configures the serializer used for not only shuffling data 
        // between worker nodes but also when serializing RDDs to disk. Another 
        // requirement for Kryo serializer is to register the classes in advance 
        // for best performance. If the classes are not registered, then the kryo 
        // would store the full class name with each object (instead of mapping 
        // with an ID), which can lead to wasted resource.
        sparkConf.set("spark.kryo.registrator", "org.apache.spark.serializer.KryoRegistrator");   
        //
        return context;
    }     
    
    /**
     * @param args arguments to run the program
     * 
     * args[0] = K > 0, select K smallest outliers
     * args[1] = input path
     */
    public static void main(String[] args) throws Exception {
        //
        // Step-1: handle input parameters
        // make sure we have 2 arguments
        if (args.length != 2) {
            THE_LOGGER.warn("Usage: OutlierDetection <K> <input-path>");
            System.exit(1);
        }
        final int K = Integer.parseInt(args[0]);
        final String inputPath = args[1];
        THE_LOGGER.info("K="+K);
        THE_LOGGER.info("inputPath="+inputPath);
        

        // Step-2: create a spark context and then read input and create the first RDD
        // create a context object, which is used 
        // as a factory for creating new RDDs
        JavaSparkContext context = createJavaSparkContext(); // 
        //read input (as categorical dataset) and create the first RDD       
        JavaRDD<String> records = context.textFile(inputPath);   
        records.cache(); // cache it: since we are goint to use it again
        
        // Step-3: perform the map() for each RDD element
        // for each input record of: <record-id><,><data1><,><data2><,><data3><,>...
        //    emit(data1, 1)
        //    emit(data2, 1)
        //    emit(data3, 1)
        //    ...
        //
	// PairFunction<T, K, V>	
	// T => Tuple2<K, V>
        JavaPairRDD<String,Integer> ones = records.flatMapToPair((String rec) -> {
            //
            List<Tuple2<String,Integer>> results = new ArrayList<Tuple2<String,Integer>>();
            // rec has the following format:
            // <record-id><,><data1><,><data2><,><data3><,>...
            String[] tokens = StringUtils.split(rec, ",");
            for (int i=1; i < tokens.length; i++) {
                results.add(new Tuple2<String,Integer>(tokens[i], 1));
            }
            return results.iterator();
        });
        
        // Step-4: find frequencies of all categirical data (keep categorical-data as String)
        JavaPairRDD<String, Integer> counts = 
                ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);    
        
        // Step-5: build an associative array to be used for finding AVF Score
        // public java.util.Map<K,V> collectAsMap()
        // Return the key-value pairs in this RDD to the master as a Map.   
        final Map<String, Integer> map = counts.collectAsMap();
        
        
        // Step-6: compute AVF Score using the built associative array
        JavaPairRDD<String,Double> avfScore = records.mapToPair((String rec) -> {
            //
            // rec has the following format:
            // <record-id><,><data1><,><data2><,><data3><,>...
            String[] tokens = StringUtils.split(rec, ",");
            String recordID = tokens[0];
            int sum = 0;
            for (int i=1; i < tokens.length; i++) {
                sum += map.get(tokens[i]);
            }
            double m = (double) (tokens.length -1);
            double avfScore1 = ((double) sum) / m;
            return new Tuple2<String,Double>(recordID, avfScore1);
        });
        
        // Step-7: take the lowest K AVF scores
        // java.util.List<T> takeOrdered(int K)
        // Returns the first K (smallest) elements from this RDD using 
        // the natural ordering for T while maintain the order.        
        List<Tuple2<String,Double>> outliers = avfScore.takeOrdered(K, TupleComparatorAscending.INSTANCE);       
        System.out.println("Ascending AVF Score:");
        System.out.println(outliers);
        
        //List<Tuple2<String,Double>> outliers2 = avfScore.takeOrdered(K, TupleComparatorDescending.INSTANCE);       
        //System.out.println("descending");
        //System.out.println(outliers2);
           
        // Step-8: done & close the spark context
        context.close();
    }

}
