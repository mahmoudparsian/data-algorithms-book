package org.dataalgorithms.machinelearning.logistic.alcohol;

import org.apache.commons.lang.StringUtils;
//
import org.apache.log4j.Logger;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;


/**
 * Input format:
 *      feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23
 *      classification column: 20
 * 
 * Classification values: {1, 2,3,4,5}
 * 
 *# awk 'BEGIN{FS=","}{print $20}' data/student_alcohol_training_data.txt | sort | grep 1 | wc -l
 *    249
 *# awk 'BEGIN{FS=","}{print $20}' data/student_alcohol_training_data.txt | sort | grep 2 | wc -l
 *    67
 *# awk 'BEGIN{FS=","}{print $20}' data/student_alcohol_training_data.txt | sort | grep 3 | wc -l
 *     19
 *# awk 'BEGIN{FS=","}{print $20}' data/student_alcohol_training_data.txt | sort | grep 4 | wc -l
 *      6
 *# awk 'BEGIN{FS=","}{print $20}' data/student_alcohol_training_data.txt | sort | grep 5 | wc -l
 *     8
 * * 
 * @author Mahmoud Parsian (mparsian@yahoo.com)
 *
 */
public final class StudentAlcoholDetectionBuildModel {
    
    private static final Logger THE_LOGGER = Logger.getLogger(StudentAlcoholDetectionBuildModel.class);
 
    public static void main(String[] args) {
        //
        Util.debugArguments(args);
        //
        if (args.length != 2) {
            throw new RuntimeException("usage: StudentAlcoholDetectionBuildModel <training-data-set-path> <built-model-path>");
        }

        //
        String trainingInputPath = args[0];
        String builtModelPath = args[1];
        THE_LOGGER.info("trainingInputPath=" + trainingInputPath);
        THE_LOGGER.info("builtModelPath=" + builtModelPath);
        
        
        // create a Factory context object
        JavaSparkContext context = Util.createJavaSparkContext("StudentAlcoholDetectionBuildModel");

        // Each line has a breast cancer record
        JavaRDD<String> records = context.textFile(trainingInputPath); // e.g.: /breastcancer/input/breast-cancer-wisconsin-wdbc-data.txt

        // feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23
        // classification column: 20
        JavaRDD<LabeledPoint> training = records.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String record) {
                //
                String[] tokens = StringUtils.split(record, ","); 
                //
                double[] features = new double[14];
                //
                features[0] = Double.parseDouble(tokens[0]);
                features[1] = Double.parseDouble(tokens[1]);
                features[2] = Double.parseDouble(tokens[2]);
                features[3] = Double.parseDouble(tokens[3]);
                features[4] = Double.parseDouble(tokens[4]);
                features[5] = Double.parseDouble(tokens[6]);
                features[6] = Double.parseDouble(tokens[7]);
                features[7] = Double.parseDouble(tokens[8]);
                features[8] = Double.parseDouble(tokens[11]);
                features[9] = Double.parseDouble(tokens[13]);
                features[10] = Double.parseDouble(tokens[16]);
                features[11] = Double.parseDouble(tokens[17]);
                features[12] = Double.parseDouble(tokens[18]);
                features[13] = Double.parseDouble(tokens[22]);
                // 
                Vector vector = new DenseVector(features);
                //
                String classLevel = tokens[19];
                THE_LOGGER.info("training data: classLevel=" + classLevel);
                double classification = getClassification5(classLevel);
                THE_LOGGER.info("training data: classification=" + classification);
                //
                return new LabeledPoint(classification, vector); 
            }
        });

        // Split initial RDD into two... [60% training data, 40% testing data].
        //JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        //JavaRDD<LabeledPoint> training = splits[0].cache();
        //JavaRDD<LabeledPoint> test = splits[1];        
        // Cache data since Logistic Regression is an iterative algorithm.
        training.cache(); 
        

        // Run training algorithm to build the model.
        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
            .setNumClasses(5)
            .run(training.rdd());


        // SAVE the MODEL: for the future use
        //
        //   you may save the model for future use:
        //   public void save(SparkContext sc, String path)
        //   Description copied from interface: Saveable
        //   Save this model to the given path.
        model.save(context.sc(), builtModelPath);
        
        
        // Compute raw scores on the test set.
        /*
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
          new Function<LabeledPoint, Tuple2<Object, Object>>() {
            public Tuple2<Object, Object> call(LabeledPoint p) {
              Double prediction = model.predict(p.features());
              System.out.println("prediction="+prediction+"\t p.label()="+p.label());
              return new Tuple2<Object, Object>(prediction, p.label());
            }
          }
        );
        */

        
        
        // Then later, you may LOAD the MODEL from saved PATH:
        //
        //   later on you may load the model from the saved path
        //   public static LogisticRegressionModel load(SparkContext sc, String path)
                
        
        // done
        context.stop();
    }
    
    private static double getClassification2(String classLevel) {
        if (classLevel.equals("1") | classLevel.equals("2") | classLevel.equals("3")) {
            // classLevel is "mild" and in {"1", "2", "3"}
            return 0.0;

        } 
        else {
            // classLevel is severe and in {"4", "5" }
            return 1.0;
        }
    }
    
    /**
     * Classify from 0 to K-1 when K is the number of classifications
     * 
     * @param classLevel
     * @return 
     */
    private static double getClassification5(String classLevel) {
        if (classLevel.equals("1")) {
            return 0.0;
        }
        else if (classLevel.equals("2")) {
            return 1.0;
        }        
        else if (classLevel.equals("3")) {
            return 2.0;
        }        
        else if (classLevel.equals("4")) {
            return 3.0;
        } 
        else {
            // must be "5"
            return 4.0;
        }
    }    
}