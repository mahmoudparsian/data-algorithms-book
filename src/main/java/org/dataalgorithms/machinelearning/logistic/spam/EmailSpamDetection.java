package org.dataalgorithms.machinelearning.logistic.spam;


import scala.Tuple2;
//
import java.util.Arrays;
//
import org.apache.log4j.Logger;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.classification.LogisticRegressionModel;

/**
 * This example shows how to use the built logistic regression 
 * model (built by Spark's MLlib -- EmailSpamDetectionBuildModel) 
 * to classify emails into two categories: {spam, non-spam}.
 * 
 * We read two input:
 * -- the path for the built model
 * -- query data path
 * 
 * First, We load the model from the saved path and then 
 * classify the query data.
 * 
 * 
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public final class EmailSpamDetection {

    private static final Logger THE_LOGGER = Logger.getLogger(EmailSpamDetection.class);   

    public static void main(String[] args) {
        Util.debugArguments(args);
        if (args.length != 2) {
            throw new RuntimeException("usage: EmailSpamDetection <query-path> <saved-model-path>");
        }

        //
        String queryInputPath = args[0];
        String savedModelPath = args[1];
        THE_LOGGER.info("queryInputPath=" + queryInputPath);
        THE_LOGGER.info("savedModelPath=" + savedModelPath);

        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("EmailSpamDetection");

        
        // LOAD the MODEL from saved PATH:
        //
        //   public static LogisticRegressionModel load(SparkContext sc, String path)
        final LogisticRegressionModel model = LogisticRegressionModel.load(context.sc(), savedModelPath);
        
        // Read query types of emails from text files
        // Each line has text from one email.
        JavaRDD<String> query = context.textFile(queryInputPath);

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
        final HashingTF tf = new HashingTF(100);


        // now that we have a logistic regression model, we can query  
        // the model to see how it responds.
        //
        // we create JavaPairRDD<String, Double> where ._1 is an email 
        // as a String and ._2 is classification we get from the logistic 
        // regression model
        /*
        JavaPairRDD<String, Double> classifications = query.mapToPair(
            new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String email) {
                Vector v = tf.transform(Arrays.asList(email.split(" ")));
                double classification = model.predict(v);
                THE_LOGGER.info("email="+email);
                THE_LOGGER.info("classification="+classification);
                //
                return new Tuple2<String, Double>(email, classification);
            }
        });
        */
        
        JavaPairRDD<String, Double> classifications = query.mapToPair((String email) -> {
            Vector v = tf.transform(Arrays.asList(email.split(" ")));
            double classification = model.predict(v);
            THE_LOGGER.info("email="+email);
            THE_LOGGER.info("classification="+classification);
            //
            return new Tuple2<String, Double>(email, classification);
        });        
        
        
        
        //
        // for debugging purposes: print the results
        //
        Iterable<Tuple2<String, Double>> predictions = classifications.collect();
        for (Tuple2<String, Double> pair : predictions) {
            THE_LOGGER.info("query email="+pair._1);
            THE_LOGGER.info("prediction="+pair._2);
        }
    
             
        // done
        context.stop();
    }
       
}
