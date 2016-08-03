package org.dataalgorithms.machinelearning.naivebayes.tennis;

import scala.Tuple2;
//
import org.apache.log4j.Logger;
//
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
//
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;


/**
  
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

 
NaiveBayes implements multinomial Naive Bayes. 
It takes an RDD of LabeledPoint and an optionally 
smoothing parameter lambda as input, and outputs a 
NaiveBayesModel, which can be used for evaluation 
and prediction.


@author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 
 */
public class PredictTennisPlayWithLambda {

    private static final Logger THE_LOGGER = Logger.getLogger(PredictTennisPlayWithLambda.class);

    public static void main(String[] args) throws Exception {
        Util.printArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: PredictDiabetes <saved-model-path> <query-data-path>");
        }

        //
        String savedModelPath = args[0];
        String queryDataPath = args[1];
        THE_LOGGER.info("--- savedModelPath=" + savedModelPath);
        THE_LOGGER.info("--- queryDataPath=" + queryDataPath);

        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("PredictDiabetes");

        //
        // create query data set
        // input records format: <feature-1><,><feature-2><feature-3><,><feature-4>
        //
        JavaRDD<String> queryRDD = context.textFile(queryDataPath);
        JavaRDD<Vector> query = Util.createFeatureVector(queryRDD);

        //
        // load the built model from the saved path
        //
        final NaiveBayesModel model = NaiveBayesModel.load(context.sc(), savedModelPath);

        //
        // predict the query data
        // JavaPairRDD<Vector, Double> = JavaPairRDD<Vector as input, Double prediction as output>
        //
        JavaPairRDD<Vector, Double> predictionAndLabel
                = query.mapToPair((Vector v) -> {
                    // predict values for a single data point using the model trained.
                    double prediction = model.predict(v);
                    return new Tuple2<Vector, Double>(v, prediction);
                });

        //
        // DEBUG/VIEW predictions:
        //
        Iterable<Tuple2<Vector, Double>> predictions = predictionAndLabel.collect();
        for (Tuple2<Vector, Double> p : predictions) {
            THE_LOGGER.info("input: " + p._1);
            THE_LOGGER.info("prediction: " + p._2);
        }

        // done
        context.close();
    }

}
