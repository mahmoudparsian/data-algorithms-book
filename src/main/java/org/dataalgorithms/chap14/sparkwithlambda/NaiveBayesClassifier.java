package org.dataalgorithms.chap14.sparkwithlambda;


// STEP-0: import required classes and interfaces
import java.util.Map;
import java.util.List;
//
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
//
import edu.umd.cloud9.io.pair.PairOfStrings;
//
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.io.DoubleWritable;
//
import org.dataalgorithms.util.SparkUtil;
 
/**
 * Naive Bayes Classifier, which classifies (using the 
 * classifier built by the BuildNaiveBayesClassifier class) 
 * new data. 
 *
 * Now for a given X = (X1, X2, ..., Xm), we will classify it 
 * by using the following data structures (built by the 
 * BuildNaiveBayesClassifier class):
 *
 *     ProbabilityTable(c) = p-value where c in C
 *     ProbabilityTable(c, a) = p-value where c in C and a in A
 *
 * Therefore, given X, we will classify it as C where C in {C1, C2, ..., Ck}
 *
 * @author Mahmoud Parsian
 */
public class NaiveBayesClassifier implements java.io.Serializable {

   /**
    * record example in new data (to be classified) : 
    *    <attribute_1><,><attribute_2><,>...<,><attribute_m>
    *
    */
   public static void main(String[] args) throws Exception {
   
      // STEP-1: handle input parameters
      if (args.length != 2) {
         System.err.println("Usage: NaiveBayesClassifier <input-data-filename> <NB-PT-path> ");
         System.exit(1);
      }
      final String inputDataFilename = args[0];
      final String nbProbTablePath = args[1];

      // STEP-2: create a Spark context object
      JavaSparkContext ctx = SparkUtil.createJavaSparkContext("naive-bayes-classifier");

      // STEP-3: read new data to be classified
      JavaRDD<String> newdata = ctx.textFile(inputDataFilename, 1);  
      
      // STEP-4: read the classifier from hadoop
      //  JavaPairRDD<K,V> hadoopFile(String path,
      //                              Class<F> inputFormatClass,
      //                              Class<K> keyClass,
      //                              Class<V> valueClass)
      // Get an RDD for a Hadoop file with an arbitrary InputFormat
      // '''Note:''' Because Hadoop's RecordReader class re-uses the 
      // same Writable object for each record, directly caching the 
      // returned RDD will create many references to the same object. 
      // If you plan to directly cache Hadoop writable objects, you 
      // should first copy them using a map function.      
      JavaPairRDD<PairOfStrings, DoubleWritable> ptRDD = ctx.hadoopFile(
                          nbProbTablePath,             	    // "/naivebayes/pt" 
                          SequenceFileInputFormat.class,    // input format class
                          PairOfStrings.class,              // key class
                          DoubleWritable.class              // value class	
                         );          

      // <K2,V2> JavaPairRDD<K2,V2> mapToPair(PairFunction<T,K2,V2> f)
      // Return a new RDD by applying a function to all elements of this RDD.
      JavaPairRDD<Tuple2<String,String>, Double> classifierRDD = 
              ptRDD.mapToPair((Tuple2<PairOfStrings,DoubleWritable> rec) -> {
          PairOfStrings pair = rec._1;
          Tuple2<String,String> K2 = new Tuple2<String,String>(pair.getLeftElement(), pair.getRightElement());
          Double V2 = rec._2.get();
          return new Tuple2<Tuple2<String,String>,Double>(K2, V2);
      });    
      
      // STEP-5: cache the classifier components, which can be used from any node in the cluster.
      Map<Tuple2<String,String>, Double> classifier = classifierRDD.collectAsMap();
      final Broadcast<Map<Tuple2<String,String>, Double>> broadcastClassifier = ctx.broadcast(classifier);
      // we need all classifications classes, which was created by the 
      // BuildNaiveBayesClassifier class.
      JavaRDD<String> classesRDD = ctx.textFile("/naivebayes/classes", 1);
      List<String> CLASSES = classesRDD.collect();
      final Broadcast<List<String>> broadcastClasses = ctx.broadcast(CLASSES);
      
      
      // STEP-6: classify new data      
      // Now, we have Naive Bayes classifier and new data
      // Use the classifier to classify new data
      // PairFlatMapFunction<T, K, V>
      // T => Iterable<Tuple2<K, V>>
      // K = <CLASS,classification> or <attribute,classification>
      //
      // T = A1,A2, ...,Am (data to be classified)
      // K = A1,A2, ...,Am (data to be classified)
      // V = classification for T     
      JavaPairRDD<String,String> classified = newdata.mapToPair((String rec) -> {
          // get the classifer from the cache
          Map<Tuple2<String,String>, Double> CLASSIFIER = broadcastClassifier.value();
          // get the classes from the cache
          List<String> CLASSES1 = broadcastClasses.value();
          // rec = (A1, A2, ..., Am)
          String[] attributes =  rec.split(",");
          String selectedClass = null;
          double maxPosterior = 0.0;
          for (String aClass : CLASSES1) {
              double posterior = CLASSIFIER.get(new Tuple2<String,String>("CLASS", aClass));
              for (int i=0; i < attributes.length; i++) {
                  Double probability = CLASSIFIER.get(new Tuple2<String,String>(attributes[i], aClass));
                  if (probability == null) {
                      posterior = 0.0;
                      break;
                  } 
                  else {
                      posterior *= probability.doubleValue();
                  }
              }
              if (selectedClass == null) {
                  // computing values for the first classification
                  selectedClass = aClass;
                  maxPosterior = posterior;
              }
              else {
                  if (posterior > maxPosterior) {
                      selectedClass = aClass;
                      maxPosterior = posterior;
                  }
              }
          }
          return new Tuple2<String,String>(rec, selectedClass);
      }); 
         
      classified.saveAsTextFile("/output/classified");
      
      // done
      ctx.close();     
      System.exit(0);
   }

}
/*
PT={
    (Normal,No)=0.2, 
    (Mild,Yes)=0.4444444444444444, 
    (Normal,Yes)=0.6666666666666666, 
    (Overcast,Yes)=0.4444444444444444, 
    (CLASS,No)=0.35714285714285715, 
    (CLASS,Yes)=0.6428571428571429, 
    (Hot,Yes)=0.2222222222222222, 
    (Hot,No)=0.4, 
    (Cool,No)=0.2, 
    (Sunny,No)=0.6, 
    (High,No)=0.8, 
    (Rain,No)=0.4, 
    (Sunny,Yes)=0.2222222222222222, 
    (Cool,Yes)=0.3333333333333333, 
    (Rain,Yes)=0.3333333333333333, 
    (Mild,No)=0.4, 
    (High,Yes)=0.3333333333333333
   }
   
# hadoop fs -text /classifier.seq/part*
(Normal, No)	0.2
(Mild, Yes)	0.4444444444444444
(Normal, Yes)	0.6666666666666666
(Overcast, Yes)	0.4444444444444444
(CLASS, No)	0.35714285714285715
(CLASS, Yes)	0.6428571428571429
(Hot, Yes)	0.2222222222222222
(Hot, No)	0.4
(Cool, No)	0.2
(Sunny, No)	0.6
(High, No)	0.8
(Rain, No)	0.4
(Sunny, Yes)	0.2222222222222222
(Cool, Yes)	0.3333333333333333
(Rain, Yes)	0.3333333333333333
(Mild, No)	0.4
(High, Yes)	0.3333333333333333  

# hadoop fs -cat /output/classified/part*
(Rain,Hot,High,Strong,Yes)
(Overcast,Mild,Normal,Weak,Yes)
(Sunny,Mild,Normal,Week,Yes) 

*/
