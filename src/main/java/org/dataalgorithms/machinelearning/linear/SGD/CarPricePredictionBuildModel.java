package org.dataalgorithms.machinelearning.linear.SGD;

import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

/**
 * Input format:
 * <Price><,><Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
 * 
 * where <Price> is the classification column.
 * 
 * @author Mahmoud Parsian (mparsian@yahoo.com)
 *
 */
public final class CarPricePredictionBuildModel {
    
    private static final Logger THE_LOGGER = Logger.getLogger(CarPricePredictionBuildModel.class);
 

    public static void main(String[] args) throws Exception {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: CarPricePredictionBuildModel <training-data-set-path> <built-model-path>");
        }

        //
        String trainingInputPath = args[0];
        String builtModelPath = args[1];
        THE_LOGGER.info("trainingInputPath=" + trainingInputPath);
        THE_LOGGER.info("builtModelPath=" + builtModelPath);
        
        
        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("CarPricePredictionBuildModel");

        // Each line has a breast cancer record
        JavaRDD<String> records = context.textFile(trainingInputPath); // e.g.: /toyota/training/ToyotaCorolla_Transformed.csv


        // Create LabeledPoint datasets for car prices
        JavaRDD<LabeledPoint> trainingData = Util.buildLabeledPointRDD(records);
        
        // Cache data since Logistic Regression is an iterative algorithm.
        trainingData.cache(); 
 
        // Building the model
        // final double stepSize = 0.0000001;  // worked! no Nan, but wron predictions
        // final int numberOfIterations = 100; // worked! no Nan, but wron predictions
        //
        //final double stepSize = 0.0000000009;   // worked, produces prices lower than actual prices    
        //final int numberOfIterations = 100;     // worked, produces prices lower than actual prices
        //      
        final double stepSize = 0.0000000009;   // 
        final int numberOfIterations = 40;      // 
        // 
        
        // 1.6.1
        //final LinearRegressionModel model = new LinearRegressionWithSGD(stepSize, numberOfIterations, 1.0)
        //        .setIntercept(true)
        //        .run(JavaRDD.toRDD(trainingData));
        
        // 2.0.0
        final LinearRegressionModel model =
              LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingData), numberOfIterations, stepSize);

        
        //
        THE_LOGGER.info("LinearRegressionModel weights: " + model.weights());
        THE_LOGGER.info("LinearRegressionModel intercept: " + model.intercept());
        
        
        // SAVE the MODEL: for the future use
        //
        //   you may save the model for future use:
        //   public void save(SparkContext sc, String path)
        //   Description copied from interface: Saveable
        //   Save this model to the given path.
        model.save(context.sc(), builtModelPath);
        THE_LOGGER.info("model saved at: builtModelPath=" + builtModelPath);
       
        
        // Then later, you may LOAD the MODEL from saved PATH:
        //
        //   later on you may load the model from the saved path
        //   public static LogisticRegressionModel load(SparkContext sc, String path)
                
        
        // done
        context.stop();
        THE_LOGGER.info("done!");
    }
}