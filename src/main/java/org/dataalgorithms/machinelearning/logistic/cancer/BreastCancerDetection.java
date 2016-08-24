package org.dataalgorithms.machinelearning.logistic.cancer;

import org.apache.log4j.Logger;
//
import org.apache.commons.lang.StringUtils;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
//
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.classification.LogisticRegressionModel;

/**
 * Input format:
 * <patient-id><,><feature-1><,><feature-2>...<feature-30>
 * 
 * The goal is to use the built logistic regression model and
 * to classify each record into B (Benign) or M (Malignant)
 *     
 * 
 * 
 * @author Mahmoud Parsian (mparsian@yahoo.com)
 *
 */
public final class BreastCancerDetection {
    
    private static final Logger THE_LOGGER = Logger.getLogger(BreastCancerDetection.class);

    /**
     * Build a pair of (patientID, Vector), where Vector is a featurized vector
     * 
     * @param record a single patient record, which has the following format:
     *   <patient-id><,><feature-1><,><feature-2>...<feature-30>
     * @return a Tuple2<String, Vector> = Tuple2<patientID, featurized-Vector>
     * 
     */ 
    static Tuple2<String, Vector>  buildVector(String record) {
        double[] features = new double[30];
        String[] tokens = StringUtils.split(record, ","); // 31 tokens
        String patientID = tokens[0];
        for (int i = 1; i < features.length; i++) {
            features[i-1] = Double.parseDouble(tokens[i]);
        }
        //
        Vector v = new DenseVector(features);
        return new Tuple2(patientID, v);
    }    
    

    public static void main(String[] args) {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: BreastCancerDetection <query-path> <saved-model-path>");
        }

        //
        String queryInputPath = args[0];
        String savedModelPath = args[1];
        THE_LOGGER.info("queryInputPath=" + queryInputPath);
        THE_LOGGER.info("savedModelPath=" + savedModelPath);
        
        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("BreastCancerDetection");

        // Each line has a breast cancer record
        // Input format:
        //      <patient-id><,><feature-1><,><feature-2>...<feature-30>
        //
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
        //      we create JavaPairRDD<String, Double> where ._1 is a patientID 
        //      as a String and ._2 is classification we get from the logistic 
        //      regression model
        //
        JavaPairRDD<String, Double> classifications = query.mapToPair(
            new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String record) {    
                // each record has this format:
                //      <patient-id><,><feature-1><,><feature-2>...<feature-30>
                Tuple2<String, Vector>  pair = buildVector(record);
                Vector v  = pair._2;
                String patientID = pair._1;
                double classification = model.predict(v);
                THE_LOGGER.info("patientID="+patientID);
                THE_LOGGER.info("classification="+classification);
                //
                return new Tuple2<String, Double>(patientID, classification);
            }
        });
        

        //
        // for debugging purposes: print the results
        //
        Iterable<Tuple2<String, Double>> predictions = classifications.collect();
        for (Tuple2<String, Double> pair : predictions) {
            THE_LOGGER.info("query: patientID="+pair._1);
            THE_LOGGER.info("prediction="+pair._2);
        }        
        
        // done
        context.stop();
    }
}
