package org.dataalgorithms.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This is a utility class to create JavaSparkContext 
 * and other objects required by Spark. There are many 
 * ways to create JavaSparkContext object. Here we offer 
 * 2 ways to create it:
 *
 *   1. by using YARN's resource manager host name
 *
 *   2. by using spark master URL, which is expressed as: 
 *
 *           spark://<spark-master-host-name>:7077
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkUtil {

   /**
    * Create a JavaSparkContext that loads settings from system properties 
    * (for instance, when launching with ./bin/spark-submit).
    *
    * @return a JavaSparkContext
    *
    */
    public static JavaSparkContext createJavaSparkContext()  
      throws Exception {
      return new JavaSparkContext();   
   }

   /**
    * Create a JavaSparkContext object from a given Spark's master URL
    *
    * @param sparkMasterURL Spark master URL as "spark://<spark-master-host-name>:7077"
    * @param applicationName application name
    * @return a JavaSparkContext
    *
    */
   public static JavaSparkContext createJavaSparkContext(String sparkMasterURL, String applicationName) 
      throws Exception {
      JavaSparkContext ctx = new JavaSparkContext(sparkMasterURL, applicationName);   
      return ctx;
   }
   
    
   
   /**
    * Create a JavaSparkContext that loads settings from system properties 
    * (for instance, when launching with ./bin/spark-submit).
    * @param applicationName application name
    *
    * @return a JavaSparkContext
    *
    */
    public static JavaSparkContext createJavaSparkContext(String applicationName)  
      throws Exception { 
      SparkConf conf = new SparkConf().setAppName(applicationName);
      JavaSparkContext ctx = new JavaSparkContext(conf);
      return ctx;
   }
   
   public static String version() {
      return "2.0.0";
   }   
}
