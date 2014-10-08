package org.dataalgorithms.util;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This is a utitity class to create JavaSparkContext 
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
    * Create a JavaSparkContext object from a given YARN's resource manager host
    *
    * @param yarnResourceManagerHost the YARN's resource manager host
    * @return a JavaSparkContext
    *
    */
   public static JavaSparkContext createJavaSparkContext(String yarnResourceManagerHost) 
      throws Exception {
      SparkConf conf = new SparkConf();
      conf.set("yarn.resourcemanager.hostname", yarnResourceManagerHost);
      conf.set("yarn.resourcemanager.scheduler.address", yarnResourceManagerHost + ":8030");
      conf.set("yarn.resourcemanager.resource-tracker.address", yarnResourceManagerHost + ":8031");
      conf.set("yarn.resourcemanager.address",  yarnResourceManagerHost + ":8032");
      conf.set("mapreduce.framework.name", "yarn");
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      conf.set("spark.executor.memory", "1g");
      JavaSparkContext ctx = new JavaSparkContext("yarn-cluster", "spark-program", conf);
      return ctx;
   }

   /**
    * Create a JavaSparkContext object from a given Spark's master URL
    *
    * @param sparkMasterURL Spark master URL as "spark://<spark-master-host-name>:7077"
    * @param description program description
    * @return a JavaSparkContext
    *
    */
   public static JavaSparkContext createJavaSparkContext(String sparkMasterURL, String description) 
      throws Exception {
      JavaSparkContext ctx = new JavaSparkContext(sparkMasterURL, description);   
      return ctx;
   }
   
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
   
   public static String version() {
      return "1.0.0";
   }   
}
