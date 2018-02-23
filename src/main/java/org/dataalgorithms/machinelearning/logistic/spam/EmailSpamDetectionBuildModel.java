package org.dataalgorithms.machinelearning.logistic.spam;

import java.util.Arrays;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;

/**
 * This example shows how to use the Spark's MLlib
 * to build a "model" for logistic regression.
 * 
 * The training data is comprised of two types of data: spam and non-spam.
 * We read two types of email training data (spam and non-spam). 
 * -- Spam emails are considered positive (assign label/classification of 1)
 * -- Non-spam emails are considered negative (assign label/classification of 0)
 * 
 * We load these from training emails directory and label (classify) 
 * them accordingly.  Next, we create logistic regression model and 
 * finally we test the model by some sample query emails.
 * 
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public final class EmailSpamDetectionBuildModel {

    private static final Logger THE_LOGGER = Logger.getLogger(EmailSpamDetectionBuildModel.class);
    

    public static void main(String[] args) {
        //
        Util.debugArguments(args);
        //
        if (args.length != 3) {
            throw new RuntimeException("usage: EmailSpamDetectionBuildModel <spam-path> <non-spam-path> <built-model-path>");
        }

        //
        String spamTrainingInputPath = args[0];
        String nonSpamTrainingInputPath = args[1];
        String builtModelPath = args[2];
        //
        THE_LOGGER.info("spamTrainingInputPath=" + spamTrainingInputPath);
        THE_LOGGER.info("nonSpamTrainingInputPath=" + nonSpamTrainingInputPath);
        THE_LOGGER.info("builtModelPath=" + builtModelPath);

        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("EmailSpamDetectionBuildModel");

        // Read and Load 2 types of emails from text files: spam and no-spam.
        // Each line has text from one email.
        JavaRDD<String> spam = context.textFile(spamTrainingInputPath);
        JavaRDD<String> nonSpam = context.textFile(nonSpamTrainingInputPath);

        // Note that Spark's MLlib accepts features and classifications 
        // as a set of LabeledPoint, which is a pair of "classification" 
        // and a vector/array of doubles. Note that email is a set of 
        // string tokens, which has to be featurized. Featurization mean 
        // that we convert each email into a set of features (as an array 
        // of doubles). We use HashingTF to featurize email into a set of 
        // features (HashingTF maps a sequence of terms to their term 
        // frequencies using the hashing trick).
        //
        // Create a HashingTF instance to map email text to vectors of 
        // 100 features.  NOTE: initializing the hash with HashingTF(100) 
        // notifies Spark we want every string mapped to the integers 0-99.
        //
        // HashingTF: Maps a sequence of terms to their term frequencies (TF)
        // using the hashing trick.
        //
        final HashingTF tf = new HashingTF(100);

        // Each email is split into words, and each word is mapped to one feature.  
        // Create LabeledPoint datasets for positive (spam) and negative (non-spam) 
        // examples. 
        // label = 1 => spam
        JavaRDD<LabeledPoint> spamTrainingData = createLabeledPoint(1, spam, tf);

        // create training data for non-spam (negaive lable)
        // label = 0 => non-spam
        JavaRDD<LabeledPoint> nonSpamTrainingData = createLabeledPoint(0, nonSpam, tf);
        

        // put all training data together (make a union of spam and non-spam emails):
        JavaRDD<LabeledPoint> trainingData = spamTrainingData.union(nonSpamTrainingData);

        // cache data since Logistic Regression is an iterative algorithm.
        // persist this RDD with the default storage level (`MEMORY_ONLY`)
        trainingData.cache();

        // SGD = Stochastic Gradient Descent
        //
        // Create a Logistic Regression learner which uses the SGD optimizer.
        // Train a classification model for Binary Logistic Regression using 
        // Stochastic Gradient Descent (SGD)       
        LogisticRegressionWithSGD logisticRegression = new LogisticRegressionWithSGD();

        // create a model: Run the actual learning algorithm on the training data.
        LogisticRegressionModel model = logisticRegression.run(trainingData.rdd());

        // now that we have a logistic regression model, we can query the 
        // model to see how it responds.
        // Test on a positive example (spam) and a negative one (no-spam).
        // First apply the same HashingTF feature transformation used on 
        // the training data.
        String spamEmail = "O M G GET cheap stuff by sending money to ...";
        Vector positiveTestExample = tf.transform(Arrays.asList(spamEmail.split(" ")));
        //
        // Now use the learned model to predict spam or no-spam for new emails.
        THE_LOGGER.info("Prediction for positive test example: " + model.predict(positiveTestExample));
       
        String nonspamEmail = "Hi Dad, I started studying Spark the other ...";
        Vector negativeTestExample = tf.transform(Arrays.asList(nonspamEmail.split(" ")));
        //
        // Now use the learned model to predict spam or no-spam for new emails.
        THE_LOGGER.info("Prediction for negative test example: " + model.predict(negativeTestExample));

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
    
    /**
     * 
     * @param label (as a classification) 
     *    label = 1 = positive = spam email
     *    label = 0 = negative = non-spam email
     * 
     * @param data training data 
     * @param tf an instance of HashingTF
     * @return JavaRDD<LabeledPoint>
     */
    private static JavaRDD<LabeledPoint> createLabeledPoint(
            final double label, // this is the classification
            final JavaRDD<String> data,
            final HashingTF tf) {
        JavaRDD<LabeledPoint> result = data.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String email) {
                return new LabeledPoint(label, tf.transform(Arrays.asList(email.split(" "))));
            }
        });    
        return result;
    }
                
    
}
