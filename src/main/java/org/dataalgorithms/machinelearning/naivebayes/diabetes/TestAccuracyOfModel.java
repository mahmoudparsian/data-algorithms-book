package org.dataalgorithms.machinelearning.naivebayes.diabetes;

import scala.Tuple2;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * The goal is to check the accuracy of the built model by a set of test data, 
 * which we already know their classification.
 *
 *
 * "Training Data" and "Test Data" have the same format:
 * =====================================================
 * Each record: has 9 attributes (8 features and an associated classification): 
 * 1. Number of times pregnant 
 * 2. Plasma glucose concentration a 2 hours in an oral glucose tolerance test 
 * 3. Diastolic blood pressure (mm Hg) 
 * 4. Triceps skin fold thickness (mm) 
 * 5. 2-Hour serum insulin (mu U/ml) 
 * 6. Body mass index (weight in kg/(height in m)^2) 
 * 7. Diabetes pedigree function 
 * 8. Age (years) 
 * 9. Class variable (0 or 1); the class value 1 is interpreted as "tested positive for diabetes"
 *
 *
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class TestAccuracyOfModel {

    private static final Logger THE_LOGGER = Logger.getLogger(TestAccuracyOfModel.class);

    public static void main(String[] args) throws Exception {
        Util.printArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: TestAccuracyOfModel <test-data-path> <saved-model-path> ");
        }

        //
        String testDataPath = args[0];
        String savedModelPath = args[1];
        THE_LOGGER.info("--- testDataPath=" + testDataPath);
        THE_LOGGER.info("--- savedModelPath=" + savedModelPath);

        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("TestAccuracyOfModel");

        //
        // create test data set
        // input records format: <feature-1><,>...<,><feature-8><,><classification>
        //
        JavaRDD<String> testRDD = context.textFile(testDataPath);        
        JavaRDD<LabeledPoint> test  = Util.createLabeledPointRDD(testRDD);


        //
        // load the built model from the saved path
        //
        final NaiveBayesModel model = NaiveBayesModel.load(context.sc(), savedModelPath);

        //
        // predict the test data
        //
        JavaPairRDD<Double, Double> predictionAndLabel = 
            test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
            @Override 
            public Tuple2<Double, Double> call(LabeledPoint p) {
                return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
            }
        });

        //
        // check accuracy of test data against the training data
        //
        double accuracy = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override 
            public Boolean call(Tuple2<Double, Double> pl) {
                return pl._1().equals(pl._2());
            }
        }).count() / (double) test.count();
        THE_LOGGER.info("accuracy="+accuracy);


        // done
        context.close();
    }
    
}
