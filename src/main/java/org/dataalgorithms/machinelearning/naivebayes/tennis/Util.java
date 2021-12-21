package org.dataalgorithms.machinelearning.naivebayes.tennis;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;


/**
 * This class provides some common methods.
 *
 * 
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
 *
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class Util {
    
    private static final Logger THE_LOGGER = Logger.getLogger(Util.class);
    
    /**
     * create a Factory context object
     * 
     */
    static JavaSparkContext createJavaSparkContext(String appName) {
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        return new JavaSparkContext(sparkConf);
    }
    
    
   /**
    * for debugging purposes only.
    * 
    */
    static void printArguments(String[] args) {
        if ((args == null) || (args.length == 0)) {
            THE_LOGGER.info("no arguments passed...");
            return;
        }
        for (int i=0; i < args.length; i++){
            THE_LOGGER.info("args["+i+"]="+args[i]);
        }
    }        
    
    static JavaRDD<Vector> createFeatureVector(JavaRDD<String> rdd) throws Exception {
        JavaRDD<Vector> result  = rdd.map(new Function<String, Vector>() {
            @Override
            public Vector call(String record) {
                String[] tokens = StringUtils.split(record, " "); // 4 tokens
                double[] features = new double[4];
                features[0] = getOutlook(tokens[0]);       // outlook
                features[1] = getTemperature(tokens[1]);   // temperature
                features[2] = getHumidity(tokens[2]);      // humidity 
                features[3] = getWind(tokens[3]);          // windy
                return new DenseVector(features);
            }
        }); 
        return result;
    }
    
    
    static void debug(String record, Vector v) {
        THE_LOGGER.info("DEBUG started:");
        double[] d = v.toArray();
        StringBuilder builder = new StringBuilder();
        builder.append("DEBUG[record=");
        builder.append(record);
        builder.append("]:");
        for (int i=0; i < d.length; i++){
            builder.append("\t");
            builder.append(d[i]);
        }
        THE_LOGGER.info(builder.toString());
    }
    
    static JavaRDD<LabeledPoint> createLabeledPointRDD(JavaRDD<String> rdd) throws Exception {
        JavaRDD<LabeledPoint> result  = rdd.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) {
                String[] tokens = StringUtils.split(record, " "); // 5 tokens
                double[] features = new double[4];
                features[0] = getOutlook(tokens[0]);       // outlook
                features[1] = getTemperature(tokens[1]);   // temperature
                features[2] = getHumidity(tokens[2]);      // humidity 
                features[3] = getWind(tokens[3]);          // windy
                // tokens[4] => classification: play=0 or not-play=1
                double classification = getPlay(tokens[4]); 
                Vector v = new DenseVector(features);
                debug(record, v);
                // add a classification for the training data set
                return new LabeledPoint(classification, v);
            }
        }); 
        return result;
    }

    
    // {sunny, overcast, rainy} = {1, 2, 3}
    static double getOutlook(String outlook) {
        if (outlook.equals("sunny")) {return 1.0;}
        if (outlook.equals("overcast")) {return 2.0;}
        if (outlook.equals("rainy")) {return 3.0;}
        return 0.0;
    }
    
    // {hot, mild, cool} = {1, 2, 3}
    static double getTemperature(String temperature) {
        if (temperature.equals("hot")) {return 1.0;}
        if (temperature.equals("mild")) {return 2.0;}
        if (temperature.equals("cool")) {return 3.0;}
        return 0.0;
    }  

    // {high, normal} = {1, 2}    
    static double getHumidity(String humidity) {
        if (humidity.equals("high")) {return 1.0;}
        if (humidity.equals("normal")) {return 2.0;}
        return 0.0;
    }     
    
    // {true, false} = {1, 2}
    static double getWind(String wind) {
        // {true, false} = {1, 2}
        if (wind.equals("true")) {return 1.0;}
        if (wind.equals("false")) {return 2.0;}
        return 0.0;
    }        

    // {yes, no} = {0, 1}
    static double getPlay(String play) { 
        if (play.equals("yes")) {return 0.0;}
        if (play.equals("no")) {return 1.0;}
        return 2.0;
    }            
    
    
}

