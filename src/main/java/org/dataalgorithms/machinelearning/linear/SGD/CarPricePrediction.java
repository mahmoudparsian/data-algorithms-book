package org.dataalgorithms.machinelearning.linear.SGD;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LinearRegressionModel;
/**
 * Input format:
 *     <Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
 * 
 * The goal is to use the built linear regression model and predict the price of a car.
 *     
 * 
 * 
 * @author Mahmoud Parsian (mparsian@yahoo.com)
 *
 */
public final class CarPricePrediction {
    
    private static final Logger THE_LOGGER = Logger.getLogger(CarPricePrediction.class);
    

    public static void main(String[] args) {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: CarPricePrediction <query-path> <saved-model-path>");
        }

        //
        String queryInputPath = args[0];
        String savedModelPath = args[1];
        THE_LOGGER.info("queryInputPath=" + queryInputPath);
        THE_LOGGER.info("savedModelPath=" + savedModelPath);
        
        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("CarPricePrediction");

        // Each line has a car record (without price!)
        // Input format:
        //      <Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
        //
        JavaRDD<String> query = context.textFile(queryInputPath);

        // LOAD the MODEL from saved PATH:
        //
        //   public static LinearRegressionModel load(SparkContext sc, String path)
        final LinearRegressionModel model = LinearRegressionModel.load(context.sc(), savedModelPath);
        
        //
        // predict car prices:
        //
        //      now that we have a linear regression model, we can query  
        //      the model to see how it responds.
        //
        //      we create JavaPairRDD<String, Double> where ._1 is the  
        //      entire car record as a String and ._2 is classification  
        //      we get from the linear regression model
        //
        JavaPairRDD<String, Double> classifications = query.mapToPair(
            new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String record) {    
                // each record has this format:
                //  <Age><,><KM><,><FuelType1><,><FuelType2><,><HP><,><MetColor><,><Automatic><,><CC><,><Doors><,><Weight>
                String[] tokens = StringUtils.split(record, ","); 
                double[] features = new double[tokens.length];
                for (int i = 0; i < features.length; i++) {
                    features[i] = Double.parseDouble(tokens[i]);
                }
                // 
                double carPricePrediction = model.predict(Vectors.dense(features));
                //
                return new Tuple2<String, Double>(record, carPricePrediction);
            }
        });
        

        //
        // for debugging purposes: print the results
        //
        Iterable<Tuple2<String, Double>> predictions = classifications.collect();
        for (Tuple2<String, Double> pair : predictions) {
            THE_LOGGER.info("query record="+pair._1);
            THE_LOGGER.info("carPricePrediction="+pair._2);
        }        
        
        // done
        context.stop();
    }
}
