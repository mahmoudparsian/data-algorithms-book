package org.dataalgorithms.chap14.sparkwithlambda;


// STEP-0: import required classes and interfaces
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//
import edu.umd.cloud9.io.pair.PairOfStrings;
//
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
//
import org.dataalgorithms.util.SparkUtil;

/**
 * Build Naive Bayes Classifier. 
 * The goal is to build the following data structures 
 * (probabilities and conditional probabilities) to 
 * be used in classifying new data:
 *  Let C = {C1, C2, ..., Ck} be set of classifications, 
 *  and let each training data element to have m attributes: 
 *     A = {A1, A2, ..., Am}
 *  then we will build 
 *     ProbabilityTable(c) = p-value where c in C
 *     ProbabilityTable(c, a) = p-value where c in C and a in A
 *  where    1 >= p-value >=0 
 *
 * @author Mahmoud Parsian
 */
public class NaiveBayesClassifierBuilder implements java.io.Serializable {

   static List<Tuple2<PairOfStrings, DoubleWritable>> toWritableList(Map<Tuple2<String,String>, 
                                                                     Double> PT) {
      List<Tuple2<PairOfStrings, DoubleWritable>> list = 
          new ArrayList<Tuple2<PairOfStrings, DoubleWritable>>();
      for (Map.Entry<Tuple2<String,String>, Double> entry : PT.entrySet()) { 
         list.add(new Tuple2<PairOfStrings, DoubleWritable>(
            new PairOfStrings(entry.getKey()._1,  entry.getKey()._2),
            new DoubleWritable(entry.getValue())
         ));
      }
      return list;
   }
 
   /**
    * record example in training data : 
    *    <attribute_1><,><attribute_2><,>...<,><attribute_n><,><classification>
    *
    */
   public static void main(String[] args) throws Exception {
   
      // STEP-1: handle input parameters
      if (args.length < 1) {
         System.err.println("Usage: BuildNaiveBayesClassifier <training-data-filename>");
         System.exit(1);
      }
      final String trainingDataFilename = args[0];
      
      // STEP-2: create a Spark context object
      JavaSparkContext ctx = SparkUtil.createJavaSparkContext("build-NBC");

      // STEP-3: read training data
      JavaRDD<String> training = ctx.textFile(trainingDataFilename, 1);   
      training.saveAsTextFile("/output/1");
      // get the training data size, which will be 
      // used in calculating the conditional probabilities 
      long trainingDataSize = training.count();

      // STEP-4: implement map() function to all elelments of training data      
      // PairFlatMapFunction<T, K, V>
      // T => Iterable<Tuple2<K, V>>
      // K = <CLASS,classification> or <attribute,classification>
      // A1,A2, ...,An,classification
      // K = Tuple2(CLASS,classifcation) or Tuple2(attribute,classifcation)
      // V = 1      
      JavaPairRDD<Tuple2<String,String>,Integer> pairs = training.flatMapToPair((String rec) -> {
          List<Tuple2<Tuple2<String,String>,Integer>> result =
                  new ArrayList<Tuple2<Tuple2<String,String>,Integer>>();
          String[] tokens = rec.split(",");
          // tokens[0] = A1
          // tokens[1] = A2
          // ...
          // tokens[n-1] = An
          // token[n] = classification
          int classificationIndex = tokens.length -1;
          String theClassification = tokens[classificationIndex];
          for(int i=0; i < (classificationIndex-1); i++) {
              Tuple2<String,String> K = new Tuple2<String,String>(tokens[i], theClassification);
              result.add(new Tuple2<Tuple2<String,String>,Integer>(K, 1));
          }
          
          Tuple2<String,String> K = new Tuple2<String,String>("CLASS", theClassification);
          result.add(new Tuple2<Tuple2<String,String>,Integer>(K, 1));
          return result.iterator();
      });    
      pairs.saveAsTextFile("/output/2");
      
      
      // STEP-4: implement reduce() function to all elelments of training data 
      JavaPairRDD<Tuple2<String,String>, Integer> counts = 
         pairs.reduceByKey((Integer i1, Integer i2) -> i1 + i2);      
      counts.saveAsTextFile("/output/3");
      
      // STEP-5: collect reduced data as Map
      // java.util.Map<K,V> collectAsMap()
      // Return the key-value pairs in this RDD to the master as a Map.      
      Map<Tuple2<String,String>, Integer> countsAsMap = counts.collectAsMap();
      
      // STEP-6: build the classifier data structures, which will be used
      // to classify new data; need to build the following
      //   1. the Probability Table (PT)
      //   2. the Classification List (CLASSIFICATIONS)
      Map<Tuple2<String,String>, Double> PT = new HashMap<Tuple2<String,String>, Double>();  
      List<String> CLASSIFICATIONS =   new ArrayList<String>();  
      for (Map.Entry<Tuple2<String,String>, Integer> entry : countsAsMap.entrySet()) { 
         Tuple2<String,String> k = entry.getKey();
         String classification = k._2;
         if (k._1.equals("CLASS")) {
             PT.put(k,  ( (double) entry.getValue() ) / ( (double) trainingDataSize) );  
             CLASSIFICATIONS.add(k._2);
         }
         else {
             Tuple2<String,String> k2 = new Tuple2<String,String>("CLASS", classification);
             Integer count = countsAsMap.get(k2);
             if (count == null) {
                PT.put(k, 0.0);
             }
             else {
                PT.put(k,  ( (double) entry.getValue() ) / ( (double) count.intValue()) );  
             }
         }
      }
      System.out.println("PT="+PT);
      
      // STEP-7: save the following, which will be used to classify new data
      //   1. the PT (probability table) for classification of new entries
      //   2. the Classification List (CLASSIFICATIONS)
      
      // STEP 7.1: save the PT
      // public <K,V> JavaPairRDD<K,V> parallelizePairs(java.util.List<scala.Tuple2<K,V>> list)
      // Distribute a local Scala collection to form an RDD.      
      List<Tuple2<PairOfStrings, DoubleWritable>> list  =  toWritableList(PT);
      JavaPairRDD<PairOfStrings, DoubleWritable> ptRDD = ctx.parallelizePairs(list);   
      ptRDD.saveAsHadoopFile("/naivebayes/pt",              // name of path
                          PairOfStrings.class,              // key class
                          DoubleWritable.class,             // value class
                          SequenceFileOutputFormat.class    // output format class
                         ); 

      // STEP 7.2: save the Classification List (CLASSIFICATIONS)
      // List<Text> writableClassifications = toWritableList(CLASSIFICATIONS);
      JavaRDD<String> classificationsRDD = ctx.parallelize(CLASSIFICATIONS);
      classificationsRDD.saveAsTextFile("/naivebayes/classes"); // name of path

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

# cat run_build_naive_bayes_classifier.sh 
#!/bin/bash
/bin/date
source /home/hadoop/conf/env_2.4.0.sh
export SPARK_HOME=/home/hadoop/spark-1.0.0
source /home/hadoop/spark_mahmoud_examples/spark_env_yarn.sh
source $SPARK_HOME/conf/spark-env.sh

# system jars:
CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop
#
jars=`find $SPARK_HOME -name '*.jar'`
for j in $jars ; do
   CLASSPATH=$CLASSPATH:$j
done

# app jar:
export MP=/home/hadoop/spark_mahmoud_examples
export CLASSPATH=$MP/mp.jar:$CLASSPATH
export CLASSPATH=$MP/commons-math3-3.0.jar:$CLASSPATH
export CLASSPATH=$MP/commons-math-2.2.jar:$CLASSPATH
export SPARK_CLASSPATH=$CLASSPATH
prog=NaiveBayesClassifierBuilder
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.4.0
export SPARK_LIBRARY_PATH=$HADOOP_HOME/lib/native
export JAVA_HOME=/usr/java/jdk7
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export MY_JAR=$MP/mp.jar
export SPARK_JAR=$MP/spark-assembly-1.0.0-hadoop2.4.0.jar
export YARN_APPLICATION_CLASSPATH=$CLASSPATH:$HADOOP_HOME/etc/hadoop
#export THEJARS=$MP/commons-math-2.2.jar,$MP/commons-math3-3.0.jar
export THEJARS=$MP/cloud9-1.3.2.jar
INPUT=/naivebayes/training_data.txt
RESOURCE_MANAGER_HOST=server100
$SPARK_HOME/bin/spark-submit --class $prog \
    --master yarn-cluster \
    --num-executors 12 \
    --driver-memory 3g \
    --executor-memory 7g \
    --executor-cores 12 \
    --jars $THEJARS \
    $MY_JAR  $INPUT $RESOURCE_MANAGER_HOST
/bin/date

# hadoop fs -ls /output/
Found 3 items
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /output/1
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /output/2
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /output/3

# hadoop fs -cat /output/1/part*
Sunny,Hot,High,Weak,No
Sunny,Hot,High,Strong,No
Overcast,Hot,High,Weak,Yes
Rain,Mild,High,Weak,Yes
Rain,Cool,Normal,Weak,Yes
Rain,Cool,Normal,Strong,No
Overcast,Cool,Normal,Strong,Yes
Sunny,Mild,High,Weak,No
Sunny,Cool,Normal,Weak,Yes
Rain,Mild,Normal,Weak,Yes
Sunny,Mild,Normal,Strong,Yes
Overcast,Mild,High,Strong,Yes
Overcast,Hot,Normal,Weak,Yes
Rain,Mild,High,Strong,No

# hadoop fs -cat /output/2/part*
((Sunny,No),1)
((Hot,No),1)
((High,No),1)
((CLASS,No),1)
((Sunny,No),1)
((Hot,No),1)
((High,No),1)
((CLASS,No),1)
((Overcast,Yes),1)
((Hot,Yes),1)
((High,Yes),1)
((CLASS,Yes),1)
((Rain,Yes),1)
((Mild,Yes),1)
((High,Yes),1)
((CLASS,Yes),1)
((Rain,Yes),1)
((Cool,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Rain,No),1)
((Cool,No),1)
((Normal,No),1)
((CLASS,No),1)
((Overcast,Yes),1)
((Cool,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Sunny,No),1)
((Mild,No),1)
((High,No),1)
((CLASS,No),1)
((Sunny,Yes),1)
((Cool,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Rain,Yes),1)
((Mild,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Sunny,Yes),1)
((Mild,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Overcast,Yes),1)
((Mild,Yes),1)
((High,Yes),1)
((CLASS,Yes),1)
((Overcast,Yes),1)
((Hot,Yes),1)
((Normal,Yes),1)
((CLASS,Yes),1)
((Rain,No),1)
((Mild,No),1)
((High,No),1)
((CLASS,No),1)

# hadoop fs -cat /output/3/part*
((Rain,Yes),3)
((Mild,No),2)
((Cool,No),1)
((Mild,Yes),4)
((Sunny,Yes),2)
((High,Yes),3)
((Hot,No),2)
((Sunny,No),3)
((Overcast,Yes),4)
((CLASS,No),5)
((High,No),4)
((Cool,Yes),3)
((Rain,No),2)
((Hot,Yes),2)
((CLASS,Yes),9)
((Normal,Yes),6)
((Normal,No),1)

# hadoop fs -ls /naivebayes/
Found 4 items
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /naivebayes/classes
-rw-r--r--   3 hadoop root,hadoop         70 2014-08-17 21:32 /naivebayes/new_data_to_be_classified.txt
drwxr-xr-x   - hadoop root,hadoop          0 2014-08-17 21:45 /naivebayes/pt
-rw-r--r--   3 hadoop root,hadoop        374 2014-08-15 23:39 /naivebayes/training_data.txt
                                                                                                                                
# hadoop fs -text /naivebayes/pt/part*
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

# hadoop fs -cat /naivebayes/classes/part*
Yes
No

*/
