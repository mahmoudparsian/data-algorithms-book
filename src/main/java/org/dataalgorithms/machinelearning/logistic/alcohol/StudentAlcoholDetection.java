package org.dataalgorithms.machinelearning.logistic.alcohol;

import org.apache.log4j.Logger;
//
import org.apache.commons.lang.StringUtils;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.classification.LogisticRegressionModel;

/**
 * Input format: feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23
 *
 * The goal is to use the built logistic regression model and
 * to classify each query record into 5 categories.
 *     
 * @author Mahmoud Parsian (mparsian@yahoo.com)
 *
 */
public final class StudentAlcoholDetection {
    
    private static final Logger THE_LOGGER = Logger.getLogger(StudentAlcoholDetection.class);

    /**
     * Build a featurized Vector
     * 
     * feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23
     * 
     */ 
    static Vector buildVector(String record) {
        //
        String[] tokens = StringUtils.split(record, ",");
        //
        double[] features = new double[14];
        //
        features[0] = Double.parseDouble(tokens[0]);
        features[1] = Double.parseDouble(tokens[1]);
        features[2] = Double.parseDouble(tokens[2]);
        features[3] = Double.parseDouble(tokens[3]);
        features[4] = Double.parseDouble(tokens[4]);
        features[5] = Double.parseDouble(tokens[6]);
        features[6] = Double.parseDouble(tokens[7]);
        features[7] = Double.parseDouble(tokens[8]);
        features[8] = Double.parseDouble(tokens[11]);
        features[9] = Double.parseDouble(tokens[13]);
        features[10] = Double.parseDouble(tokens[16]);
        features[11] = Double.parseDouble(tokens[17]);
        features[12] = Double.parseDouble(tokens[18]);
        features[13] = Double.parseDouble(tokens[22]);
        // 
        return new DenseVector(features);
    }

    

    public static void main(String[] args) {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: StudentAlcoholDetection <query-path> <saved-model-path>");
        }

        //
        String queryInputPath = args[0];
        String savedModelPath = args[1];
        THE_LOGGER.info("queryInputPath=" + queryInputPath);
        THE_LOGGER.info("savedModelPath=" + savedModelPath);
        
        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("BreastCancerDetection");

        // feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23
        JavaRDD<String> query = context.textFile(queryInputPath);

       // LOAD the MODEL from saved PATH:
        //
        //   public static LogisticRegressionModel load(SparkContext sc, String path)
        final LogisticRegressionModel model = LogisticRegressionModel.load(context.sc(), savedModelPath);
        
        //
        // classify:
        //
        //      now that we have a logistic regression model, we can query  
        //      the model to see how it responds.
        //
        //      we create JavaPairRDD<String, Double> where ._1 is a input
        //      as a String and ._2 is classification we get from the logistic 
        //      regression model
        //
        JavaPairRDD<String, Double> classifications = query.mapToPair((String record) -> {
            // each record has the following features:
            // feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23
            Vector  vector = buildVector(record);
            double classification = model.predict(vector);
            THE_LOGGER.info("classification="+classification);
            //
            return new Tuple2<String, Double>(record, classification);
        });
        

        //
        // for debugging purposes: print the results
        //
        Iterable<Tuple2<String, Double>> predictions = classifications.collect();
        for (Tuple2<String, Double> pair : predictions) {
            THE_LOGGER.info("query: record="+pair._1);
            THE_LOGGER.info("prediction="+pair._2);
        }        
        
        // done
        context.stop();
    }
}
