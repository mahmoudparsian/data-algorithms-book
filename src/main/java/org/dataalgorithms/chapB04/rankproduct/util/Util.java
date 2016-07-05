package org.dataalgorithms.chapB04.rankproduct.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Create JavaSparkContext and other objects.
 * 
 * Spark-1.6.1
 * spark.kryoserializer.buffer.max	64m (Default)	
 * Maximum allowable size of Kryo serialization buffer. 
 * This must be larger than any object you attempt to serialize. 
 * Increase this if you get a "buffer limit exceeded" exception inside Kryo.
 * 
 * 
  * Spark-1.6.1
* spark.kryoserializer.buffer	64k (Default)	
 * Initial size of Kryo's serialization buffer. 
 * Note that there will be one buffer per core on each worker. 
 * This buffer will grow up to spark.kryoserializer.buffer.max if needed.
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    
    public static JavaSparkContext createJavaSparkContext(boolean useYARN) {
        JavaSparkContext context;
        if (useYARN) {
            context = new JavaSparkContext("yarn-cluster", "Rank Product");
        }
        else {
            // use for NON-YARN env.
            context = new JavaSparkContext();
        } 
        
        //
        // inject efficiency
        //
        SparkConf sparkConf = context.getConf();
                
        // Now it's 32 Mb of buffer by default instead of 0.064 Mb
        sparkConf.set("spark.kryoserializer.buffer","32m");
        
        // http://www.trongkhoanguyen.com/2015/04/understand-shuffle-component-in-spark.html
        //spark.shuffle.file.buffer	32k	Size of the in-memory buffer for each 
        //                                      shuffle file output stream. These buffers 
        //                                      reduce the number of disk seeks and system 
        //                                      calls made in creating intermediate shuffle files.
        //
        sparkConf.set("spark.shuffle.file.buffer.kb","64");
        //
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
    
}
