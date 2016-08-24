package org.dataalgorithms.machinelearning.logistic.cancer;

import org.apache.commons.lang.StringUtils;
//
import org.apache.log4j.Logger;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;

/**
 * Input format:
 * <patient-id><,><tag><,><feature-1><,><feature-2>...<feature-30>
 * 
 *    where 
 *       tag in {B, M} (this is the classification of the training data set)
 *       M = Malignant
 *       B = Benign
 * 
 * 
 * @author Mahmoud Parsian (mparsian@yahoo.com)
 *
 */
public final class BreastCancerDetectionBuildModel {
    
    private static final Logger THE_LOGGER = Logger.getLogger(BreastCancerDetectionBuildModel.class);

    static Vector getMalignant() {
        // 842302,M,17.99,10.38,122.8,1001,0.1184,0.2776,0.3001,0.1471,0.2419,0.07871,1.095,0.9053,8.589,153.4,0.006399,0.04904,0.05373,0.01587,0.03003,0.006193,25.38,17.33,184.6,2019,0.1622,0.6656,0.7119,0.2654,0.4601,0.1189    
        String data = "17.99,10.38,122.8,1001,0.1184,0.2776,0.3001,0.1471,0.2419,0.07871,1.095,0.9053,8.589,153.4,0.006399,0.04904,0.05373,0.01587,0.03003,0.006193,25.38,17.33,184.6,2019,0.1622,0.6656,0.7119,0.2654,0.4601,0.1189";
        double[] features = new double[30];
        String[] tokens = StringUtils.split(data, ","); // 30 tokens
        for (int i = 0; i < features.length; i++) {
            features[i] = Double.parseDouble(tokens[i]);
        }
        return new DenseVector(features);
    }

    static Vector getBenign() {
        // 8510653,B,13.08,15.71,85.63,520,0.1075,0.127,0.04568,0.0311,0.1967,0.06811,0.1852,0.7477,1.383,14.67,0.004097,0.01898,0.01698,0.00649,0.01678,0.002425,14.5,20.49,96.09,630.5,0.1312,0.2776,0.189,0.07283,0.3184,0.08183
        String data = "13.08,15.71,85.63,520,0.1075,0.127,0.04568,0.0311,0.1967,0.06811,0.1852,0.7477,1.383,14.67,0.004097,0.01898,0.01698,0.00649,0.01678,0.002425,14.5,20.49,96.09,630.5,0.1312,0.2776,0.189,0.07283,0.3184,0.08183";
        double[] features = new double[30];
        String[] tokens = StringUtils.split(data, ","); // 30 tokens
        for (int i = 0; i < features.length; i++) {
            features[i] = Double.parseDouble(tokens[i]);
        }
        return new DenseVector(features);
    }
 

    public static void main(String[] args) {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: BreastCancerDetectionBuildModel <training-data-set-path> <built-model-path>");
        }

        //
        String trainingInputPath = args[0];
        String builtModelPath = args[1];
        THE_LOGGER.info("trainingInputPath=" + trainingInputPath);
        THE_LOGGER.info("builtModelPath=" + builtModelPath);
        
        
        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("BreastCancerDetectionBuildModel");

        // Each line has a breast cancer record
        JavaRDD<String> records = context.textFile(trainingInputPath); // e.g.: /breastcancer/input/breast-cancer-wisconsin-wdbc-data.txt


        // Create LabeledPoint datasets for malignant (label=0) and benign (label=1) 
        // NOTE: Labels used in Logistic Regression should be {0, 1, ..., k - 1} 
        // for k  [in our example, k=2]
        // classes multi-label classification problem.
        JavaRDD<LabeledPoint> trainingData = records.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) {
                String[] tokens = StringUtils.split(record, ","); // 32 tokens
                double[] features = new double[30];
                for (int i = 2; i < features.length; i++) {
                    features[i - 2] = Double.parseDouble(tokens[i]);
                }
                // String patientID = tokens[0]; // ignore, not used
                String outcomeClass = tokens[1]; // B=benign, M=malignant
                Vector v = new DenseVector(features);
                if (outcomeClass.equals("B")) {
                    return new LabeledPoint(1, v); // benign
                } 
                else {
                    return new LabeledPoint(0, v); // malignant
                }
            }
        });

        
        // Cache data since Logistic Regression is an iterative algorithm.
        trainingData.cache(); 
        
        // Create a Logistic Regression learner which uses the LBFGS optimizer.
        LogisticRegressionWithSGD learner = new LogisticRegressionWithSGD();

        // Run the actual learning algorithm on the training data.
        LogisticRegressionModel model = learner.run(trainingData.rdd());

        //
        // Test the built model on a malignent and a benign samples
        Vector malignantSample = getMalignant();
        Vector benignSample = getBenign();        
        // basic testing of the built model:
        // Now use the  model to predict Malignent or Benign
        THE_LOGGER.info("Prediction for Malignent example: " + model.predict(malignantSample));
        THE_LOGGER.info("Prediction for Benign example: " + model.predict(benignSample));
        //
        // output:
        //      Prediction for Malignent example: 0.0
        //      Prediction for Benign example: 1.0
        
        
        // SAVE the MODEL: for the future use
        //
        //   you may save the model for future use:
        //   public void save(SparkContext sc, String path)
        //   Description copied from interface: Saveable
        //   Save this model to the given path.
        model.save(context.sc(), builtModelPath);
        
        
        // Then later, you may LOAD the MODEL from saved PATH:
        //
        //   later on you may load the model from the saved path
        //   public static LogisticRegressionModel load(SparkContext sc, String path)
                
        
        // done
        context.stop();
    }
}