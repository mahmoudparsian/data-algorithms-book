package org.dataalgorithms.machinelearning.naivebayes.diabetes;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;


/**
 * The goal is to Predict the Onset of Diabetes using Naive Bayes
 * 
 * The first step is to build the model and save it.
 * Then we use NaiveBayesExamplePredictDiabetes to 
 * check new data against the built model.
 *
 *
 * Training Data: Pima Indians Diabetes
 *   https://archive.ics.uci.edu/ml/machine-learning-databases/pima-indians-diabetes/pima-indians-diabetes.data
 * 
 * 
 * The test data we will use in this tutorial is the Pima Indians 
 * Diabetes data.  This data is comprised of 768 observations of 
 * medical details for Pima indians patients. The records describe 
 * instantaneous measurements taken from the patient such as their 
 * age, the number of times pregnant and blood workup. All patients 
 * are women aged 21 or older. All attributes are numeric, and their 
 * units vary from attribute to attribute.
 * 
 * Each record has a class value that indicates whether the patient 
 * suffered an onset of diabetes within 5 years of when the measurements 
 * were taken (1) or not (0).  This is a standard dataset that has been 
 * studied a lot in machine learning literature. A good prediction accuracy 
 * is 70%-76%.
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
 *  Spark's NaiveBayes implements multinomial Naive Bayes. 
 *  It takes an RDD of LabeledPoint and an optionally smoothing 
 *  parameter lambda as input, and outputs a NaiveBayesModel, 
 *  which can be used for evaluation and prediction.
 *
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class BuildDiabetesModel {
    
    private static final Logger THE_LOGGER = Logger.getLogger(BuildDiabetesModel.class);
            

    public static void main(String[] args) throws Exception {
        Util.printArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: BuildDiabetesModel <training-data-path> <saved-path-for-built-model>");
        }

        //
        String trainingPath = args[0];
        THE_LOGGER.info("--- trainingPath=" + trainingPath);
        //
        String savedPathForBuiltModel = args[1];
        THE_LOGGER.info("--- savedPathForBuiltModel=" + savedPathForBuiltModel);

        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("BuildDiabetesModel");
        

        //
        // create training data set
        // input records format: <feature-1><,>...<,><feature-8><,><classification>
        //
        JavaRDD<String> trainingRDD = context.textFile(trainingPath);        
        JavaRDD<LabeledPoint> training  = Util.createLabeledPointRDD(trainingRDD);
        
        //
        // create a model from the given training data set
        //
        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
        
        //
        // save the built model for future use
        //
        //      public void save(SparkContext sc, java.lang.String path)
        //      Description; Save this model to the given path.       
        model.save(context.sc(), savedPathForBuiltModel);
        
        
        // done
        context.close();
    }
    
}

