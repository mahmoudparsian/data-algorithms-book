package org.dataalgorithms.machinelearning.linear.SGD;

import scala.Tuple2;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;

/**
 * Evaluate model on training examples and compute training error.
 * The goal is to check the accuracy of the built model by the same training data 
 * (which we built the linear regression model).
 *
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class ModelEvaluation {

    private static final Logger THE_LOGGER = Logger.getLogger(ModelEvaluation.class);

    public static void main(String[] args) throws Exception {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: ModelEvaluation <test-data-path> <saved-model-path> ");
        }

        //
        String testDataPath = args[0];
        String savedModelPath = args[1];
        THE_LOGGER.info("--- testDataPath=" + testDataPath);
        THE_LOGGER.info("--- savedModelPath=" + savedModelPath);

        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("TestAccuracyOfModel");

        //
        // load the built model from the saved path
        //
        final LinearRegressionModel model = LinearRegressionModel.load(context.sc(), savedModelPath);
       
        
        //
        // create test data set, data format:
        // <Price><,><Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
        //
        JavaRDD<String> testRDD = context.textFile(testDataPath);   
        
        // Create LabeledPoint datasets for car prices
        JavaRDD<LabeledPoint> test = Util.buildLabeledPointRDD(testRDD);             
        
        // Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = test.map(
            new Function<LabeledPoint, Tuple2<Double, Double>>() {
                @Override
                public Tuple2<Double, Double> call(LabeledPoint point) {
                    double prediction = model.predict(point.features());
                    return new Tuple2<Double, Double>(prediction, point.label());
                }
            }
        );
    
    
        double MSE = new JavaDoubleRDD(valuesAndPreds.map(
            new Function<Tuple2<Double, Double>, Object>() {
            @Override
                public Object call(Tuple2<Double, Double> pair) {
                    return Math.pow(pair._1() - pair._2(), 2.0);
                }
            }
        ).rdd()).mean();
        //
        //
        //
        System.out.println("training Mean Squared Error = " + MSE);

        // done
        context.close();
    }
    
}
