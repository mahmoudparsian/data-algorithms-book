package org.dataalgorithms.machinelearning.naivebayes.tennis;

import scala.Tuple2;
//
import org.apache.log4j.Logger;
//
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
//
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

/**

The goal is to check the accuracy of the built model by a set of test data, 
which we already know their classification.
 
 
"Training Data" and "Test Data" have the same format:
=====================================================
  Sample Training Data: to play tennis or not to play 
 based on four features:
 
     {outlook, temperature, humidity, windy}


Training Data:
 
outlook temperature humidity windy play
------- ----------- -------- ----- ----
sunny    hot        high     false no
sunny    hot        high     true  no
overcast hot        high     false yes
rainy    mild       high     false yes
rainy    cool       normal   false yes
rainy    cool       normal   true  no
overcast cool       normal   true  yes
sunny    mild       high     false no
sunny    cool       normal   false yes
rainy    mild       normal   false yes
sunny    mild       normal   true  yes
overcast mild       high     true  yes
overcast hot        normal   false yes
rainy    mild       high     true  no

Since LabeledPoint(double label, Vector features) 
does not accept non-numeric data (as a Vector), we 
need to convert our symbolic (non-numeric) data into 
numeric data, before building a model and then predicting 
the new data. To accomplish this, we assign numbers for 
symbolic data:

outlook = {sunny, overcast, rainy} = {1, 2, 3}
temperature = {hot, mild, cool} = {1, 2, 3}
humidity = {high, normal} = {1, 2}
windy = {true, false} = {1, 2}
play = {yes, no} = {0, 1} (the last column is the classification column)

Converting non-numeric data into numeric (as double data type): 

Training Data:
 
outlook temperature humidity    windy   play
------- ----------- --------    -----   ----
1       1           1           2       1
1       1           1           1       1
2       1           1           2       0
3       2           1           2       0
3       3           2           2       0
3       3           2           1       1
2       3           2           1       0
1       2           1           2       1
1       3           2           2       0
3       2           2           2       0
1       2           2           1       0
2       2           1           1       0
2       1           2           2       0
3       2           1           1       1

 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class TestAccuracyOfModelWithLambda {

    private static final Logger THE_LOGGER = Logger.getLogger(TestAccuracyOfModelWithLambda.class);

    public static void main(String[] args) throws Exception {
        Util.printArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: TestAccuracyOfModel <saved-model-path> <test-data-path>");
        }

        //
        String savedModelPath = args[0];
        String testDataPath = args[1];
        THE_LOGGER.info("--- savedModelPath=" + savedModelPath);
        THE_LOGGER.info("--- testDataPath=" + testDataPath);

        // create a Factory context object
        SparkConf sparkConf = new SparkConf().setAppName("TestAccuracyOfModel");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        //
        // create test data set
        // input records format: <feature-1><,><feature-2><,><feature-3><,><feature-4><,><classification>
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
            test.mapToPair((LabeledPoint p) -> new Tuple2<Double, Double>(model.predict(p.features()), p.label()));

        //
        // check accuracy of test data against the training data
        //
        double accuracy = predictionAndLabel.filter((Tuple2<Double, Double> pl) -> pl._1().equals(pl._2()))
                                            .count() / (double) test.count();
        //
        THE_LOGGER.info("accuracy="+accuracy);


        // done
        context.close();
    }
    
}
