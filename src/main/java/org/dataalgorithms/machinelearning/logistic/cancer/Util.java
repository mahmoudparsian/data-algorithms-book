package org.dataalgorithms.machinelearning.logistic.cancer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class Util {
    
    
    /**
     * for debugging purposes only.
     * 
     */
    static void debugArguments(String[] args) {
        if ((args == null) || (args.length == 0)) {
            System.out.println("no arguments passed...");
            return;
        }
        for (int i=0; i < args.length; i++){
            System.out.println("args["+i+"]="+args[i]);
        }
    }    
    
    
    /**
     * create a Factory context object
     * 
     */
    static JavaSparkContext createJavaSparkContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        return new JavaSparkContext(sparkConf);
    }
}