package org.dataalgorithms.machinelearning.linear.SGD;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Provides some basic methods and some debugging methods.
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
    
    
    static JavaRDD<LabeledPoint> buildLabeledPointRDD(JavaRDD<String> rdd) {
        // Create LabeledPoint datasets for car prices
        JavaRDD<LabeledPoint> result = rdd.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) {
                // record: <Price><,><Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
                // tokens[0] = <Price>
                String[] tokens = StringUtils.split(record, ","); 
                double[] features = new double[tokens.length - 1];
                for (int i = 0; i < features.length; i++) {
                    features[i] = Double.parseDouble(tokens[i+1]);
                }
                // 
                double price = Double.parseDouble(tokens[0]); 
                return new LabeledPoint(price, Vectors.dense(features));    
            }
        });
        //
        return result;
    }
     
}