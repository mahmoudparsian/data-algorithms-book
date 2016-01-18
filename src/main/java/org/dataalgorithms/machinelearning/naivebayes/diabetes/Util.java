package org.dataalgorithms.machinelearning.naivebayes.diabetes;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;


/**
 * This class provides some common methods for the diabetes model.
 *
 * 
 * Below is a sample from the pima-indians-diabetes.data file to get a 
 * sense of the data we will be working with.
 * 
 *  $ head -6 pima-indians-diabetes.data
 *  6,148,72,35,0,33.6,0.627,50,1
 *  1,85,66,29,0,26.6,0.351,31,0
 *  8,183,64,0,0,23.3,0.672,32,1
 *  1,89,66,23,94,28.1,0.167,21,0
 *  0,137,40,35,168,43.1,2.288,33,1
 *  5,116,74,0,0,25.6,0.201,30,0
 *
 * Each record has 9 attributes (8 features and an associated classification):
 *  1. Number of times pregnant
 *  2. Plasma glucose concentration a 2 hours in an oral glucose tolerance test
 *  3. Diastolic blood pressure (mm Hg)
 *  4. Triceps skin fold thickness (mm)
 *  5. 2-Hour serum insulin (mu U/ml)
 *  6. Body mass index (weight in kg/(height in m)^2)
 *  7. Diabetes pedigree function
 *  8. Age (years)
 *  9. Class variable (0 or 1); 
 *     -- the class value 0 is interpreted as negative
 *     -- the class value 1 is interpreted as "tested positive for diabetes"
 *
 *
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class Util {
    
    private static final Logger THE_LOGGER = Logger.getLogger(Util.class);
    
    /**
     * create a Factory context object
     * 
     */
    static JavaSparkContext createJavaSparkContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        return new JavaSparkContext(sparkConf);
    }
    
    
   /**
    * for debugging purposes only.
    * 
    */
    static void printArguments(String[] args) {
        if ((args == null) || (args.length == 0)) {
            THE_LOGGER.info("no arguments passed...");
            return;
        }
        for (int i=0; i < args.length; i++){
            THE_LOGGER.info("args["+i+"]="+args[i]);
        }
    }        

    
    static JavaRDD<LabeledPoint> createLabeledPointRDD(JavaRDD<String> rdd) throws Exception {
        JavaRDD<LabeledPoint> result  = rdd.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) {
                // 9 tokens, the last token is the classification
                String[] tokens = StringUtils.split(record, ",");
                double[] features = new double[8];
                for (int i=0; i < 8; i++) {
                    features[i] = Double.parseDouble(tokens[i]);
                } 
                //
                // tokens[8] => classification: 
                // class value 1 is interpreted as "tested positive for diabetes"
                //
                double classification = Double.parseDouble(tokens[8]);
                Vector v = new DenseVector(features);
                // debug(record, v);
                // add a classification for the training data set
                return new LabeledPoint(classification, v);
            }
        }); 
        return result;
    }
    
    static JavaRDD<Vector> createFeatureVector(JavaRDD<String> rdd) throws Exception {
        JavaRDD<Vector> result = rdd.map(new Function<String, Vector>() {
            @Override
            public Vector call(String record) {
                // 8 features
                String[] tokens = StringUtils.split(record, ",");
                double[] features = new double[8];
                for (int i = 0; i < features.length; i++) {
                    features[i] = Double.parseDouble(tokens[i]);
                }
                return new DenseVector(features);
            }
        });
        return result;
    }    
    
    static void debug(String record, Vector v) {
        THE_LOGGER.info("DEBUG started:");
        double[] d = v.toArray();
        StringBuilder builder = new StringBuilder();
        builder.append("DEBUG[record=");
        builder.append(record);
        builder.append("]:");
        for (int i=0; i < d.length; i++){
            builder.append("\t");
            builder.append(d[i]);
        }
        THE_LOGGER.info(builder.toString());
    }
    
}

