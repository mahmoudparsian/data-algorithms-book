Machine Learning Algorithms using Spark: Naive Bayes
====================================================
The purpose of this package (solutions expressed in Java 
and Spark) is to show how to implement basic machine learning 
algorithms such as Naive Bayes in Spark and Spark's MLlib 
library.  Spark's MLlib offers a suite of machine learning 
libraries for Naive Bayes and other machine learning
algorithms. This example focuses on predicting diabetes
from a given training data.


Training Data
=============
The Training Data is from [Pima Indians Diabetes](https://archive.ics.uci.edu/ml/machine-learning-databases/pima-indians-diabetes/pima-indians-diabetes.data).
The training data set, which we will use in this tutorial is the Pima Indians 
Diabetes data.  This data is comprised of 768 observations of medical details 
for Pima indians patients. The records describe instantaneous measurements taken 
from the patient such as their age, the number of times pregnant and blood workup. 
All patients are women aged 21 or older. All attributes are numeric, and their 
units vary from attribute to attribute.

Each record has a class value that indicates whether the patient 
suffered an onset of diabetes within 5 years of when the measurements 
were taken (1) or not (0).  This is a standard dataset that has been 
studied a lot in machine learning literature. A good prediction accuracy 
is 70%-76%.

Below is a sample from the pima-indians-diabetes.data file to get a 
sense of the data we will be working with.

````
$ head -6 pima-indians-diabetes.data
6,148,72,35,0,33.6,0.627,50,1
1,85,66,29,0,26.6,0.351,31,0
8,183,64,0,0,23.3,0.672,32,1
1,89,66,23,94,28.1,0.167,21,0
0,137,40,35,168,43.1,2.288,33,1
5,116,74,0,0,25.6,0.201,30,0
````

Each training data set record has 9 attributes (8 features and 
an associated classification):

````
1. Number of times pregnant
2. Plasma glucose concentration a 2 hours in an oral glucose tolerance test
3. Diastolic blood pressure (mm Hg)
4. Triceps skin fold thickness (mm)
5. 2-Hour serum insulin (mu U/ml)
6. Body mass index (weight in kg/(height in m)^2)
7. Diabetes pedigree function
8. Age (years)
9. Class variable (0 or 1); 
   -- the class value 0 is interpreted as negative
   -- the class value 1 is interpreted as "tested positive for diabetes"
````


Naive Bayes Solution
====================
This package shows how to use Spark's NaiveBayesModel for predicting diabetes.

This package has 4 classes:

1. ````BuildDiabetesModel````: builds a model from a training data (includes 
   features and classification for every data point)

 
2. ````PredictDiabetes````: use the built model and new query data to predict diabetes


3. ````TestAccuracyOfModel````: use the built model and test data (which we know the 
   classifications) to find the accuracy of the model


4. ````Util````: some common methods used in these example classes


Build Model
===========
The class ````BuildDiabetesModel```` reads the training data 
(Pima Indians Diabetes) and builds a ````NaiveBayesModel```` and saves 
it in HDFS.

To build our naive bayes model, we use a shell script: ````run_build_model_on_yarn.sh````.
After running the shell script, the model is saved in HDFS as a parquet file.


````
# ./scripts/run_build_model_on_yarn.sh
````


After the run, we can examine the model:

````
# hadoop fs -lsr /diabetes/
drwxr-xr-x   - ...          0 2015-12-28 20:01 /diabetes/model
drwxr-xr-x   - ...          0 2015-12-28 20:01 /diabetes/model/data
-rw-r--r--   1 ...          0 2015-12-28 20:01 /diabetes/model/data/_SUCCESS
-rw-r--r--   1 ...        725 2015-12-28 20:01 /diabetes/model/data/_common_metadata
-rw-r--r--   1 ...       1256 2015-12-28 20:01 /diabetes/model/data/_metadata
-rw-r--r--   1 ...       1466 2015-12-28 20:01 /diabetes/model/data/part-r-00000-e9e426df-f8fa-4422-8a90-3bc517293a34.gz.parquet
drwxr-xr-x   - ...          0 2015-12-28 20:01 /diabetes/model/metadata
-rw-r--r--   1 ...          0 2015-12-28 20:01 /diabetes/model/metadata/_SUCCESS
-rw-r--r--   1 ...        113 2015-12-28 20:01 /diabetes/model/metadata/part-00000
drwxr-xr-x   - ...          0 2015-11-19 11:07 /diabetes/training
-rw-r--r--   1 ...      23279 2015-11-19 11:07 /diabetes/training/pima-indians-diabetes.data

# cat query.txt 
11,143,94,33,146,36.6,0.254,51
10,125,70,26,115,31.1,0.205,41
7,196,90,0,0,39.8,0.451,41
9,119,80,35,0,29.0,0.263,29
1,0,48,20,0,24.7,0.140,22
7,62,78,0,0,32.6,0.391,41
5,95,72,33,0,37.7,0.370,27
2,71,70,27,0,28.0,0.586,22

The first four records have a classification of 1 and the last 4

# ./scripts/run_diabetes_prediction_on_yarn.sh 
=== begin DEBUG-by-MP ===
DEBUG-by-MP: parameter p=org.apache.spark.deploy.SparkSubmit
DEBUG-by-MP: parameter p=--class
DEBUG-by-MP: parameter p=org.dataalgorithms.machinelearning.naivebayes.diabetes.PredictDiabetes
DEBUG-by-MP: parameter p=--master
DEBUG-by-MP: parameter p=yarn-cluster
DEBUG-by-MP: parameter p=--jars
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/lib/spring-context-3.0.7.RELEASE.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/log4j-1.2.17.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/junit-4.12-beta-2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jsch-0.1.42.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/JeraAntTasks.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jedis-2.5.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jblas-1.2.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/hamcrest-all-1.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/guava-18.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math3-3.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math-2.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-logging-1.1.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang3-3.4.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang-2.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-io-2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-httpclient-3.0.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-daemon-1.0.5.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-configuration-1.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-collections-3.2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-cli-1.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/cloud9-1.3.2.jar,
DEBUG-by-MP: parameter p=--conf
DEBUG-by-MP: parameter p=spark.yarn.jar=/Users/mparsian/zmp/github/data-algorithms-book/lib/spark-assembly-1.5.2-hadoop2.5.0.jar
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar
DEBUG-by-MP: parameter p=/diabetes/query
DEBUG-by-MP: parameter p=/diabetes/model
=== done DEBUG-by-MP ===

# hadoop fs -cat /diabetes/model/metadata/part-00000
{
 "class":"org.apache.spark.mllib.classification.NaiveBayesModel",
 "version":"2.0",
 "numFeatures":8,
 "numClasses":2
}

# hadoop fs -copyToLocal /diabetes/model/data/part-r-00000-e9e426df-f8fa-4422-8a90-3bc517293a34.gz.parquet .

# java org.dataalgorithms.parquet.TestConvertParquetToCSV  part-r-00000-e9e426df-f8fa-4422-8a90-3bc517293a34.gz.parquet part.csv
15/12/28 20:27:42 INFO parquet.TestConvertParquetToCSV: parquetFile=part-r-00000-e9e426df-f8fa-4422-8a90-3bc517293a34.gz.parquet
15/12/28 20:27:42 INFO parquet.TestConvertParquetToCSV: csvOutputFile=part.csv
Dec 28, 2015 8:27:42 PM org.apache.parquet.Log info
INFO: Converting part-r-00000-e9e426df-f8fa-4422-8a90-3bc517293a34.gz.parquet to part.csv
15/12/28 20:27:43 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
15/12/28 20:27:43 INFO compress.CodecPool: Got brand-new decompressor [.gz]
15/12/28 20:27:44 INFO parquet.TestConvertParquetToCSV: estimatedTime (millis)=1645
Dec 28, 2015 8:27:43 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Dec 28, 2015 8:27:43 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: reading another 1 footers
Dec 28, 2015 8:27:43 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Dec 28, 2015 8:27:43 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 1 records.
Dec 28, 2015 8:27:43 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
Dec 28, 2015 8:27:43 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: block read in memory in 68 ms. row count = 1

# cat part.csv 
bag
  array: 0.0
bag
  array: 1.0
|bag
  array: -0.4297844137628646
bag
  array: -1.05167913524589
|bag
  array
    bag
      array: -4.610784499226044
    bag
      array: -1.1043902516595896
    bag
      array: -1.5824677025139717
    bag
      array: -2.825815711340514
    bag
      array: -1.5735904358644586
    bag
      array: -2.3933546556530203
    bag
      array: -6.644652588823429
    bag
      array: -2.364545316018182
bag
  array
    bag
      array: -4.438629060647884
    bag
      array: -1.0709897832385131
    bag
      array: -1.7613409397142412
    bag
      array: -2.9229546320489366
    bag
      array: -1.4130404432873167
    bag
      array: -2.4620819421306805
    bag
      array: -6.611773492139444
    bag
      array: -2.4087682254187612
|multinomial
mparsian@Mahmouds-MacBook:~/zmp/github/data-algorithms-book/src/main/java/org/dataalgorithms/machinelearning/naivebayes/diabetes# 
````


Output from PredictDiabetes Run
===============================
As you can see from the output, the built model did not predict all 8 query records 
correctly!!!  The following output is from the Spark logs. The first 4 should have 
been classified as 1.0 and the last four should have been classified as 0.0. The 
records 3 and 4 could have been classified as 1.0, but incorrectly have been classified 
as 0.0. This concludes that the accuracy of model will be less than 100%.


````
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [11.0,143.0,94.0,33.0,146.0,36.6,0.254,51.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 1.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [10.0,125.0,70.0,26.0,115.0,31.1,0.205,41.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 1.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [7.0,196.0,90.0,0.0,0.0,39.8,0.451,41.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 0.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [9.0,119.0,80.0,35.0,0.0,29.0,0.263,29.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 0.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [1.0,0.0,48.0,20.0,0.0,24.7,0.14,22.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 0.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [7.0,62.0,78.0,0.0,0.0,32.6,0.391,41.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 0.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [5.0,95.0,72.0,33.0,0.0,37.7,0.37,27.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 0.0
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: input: [2.0,71.0,70.0,27.0,0.0,28.0,0.586,22.0]
15/12/28 22:35:02 INFO diabetes.PredictDiabetes: prediction: 0.0
````


Check Accuracy of the Built Naive Bayes Model 
=============================================
To check the accuracy of model, we run the training data against the built model
(note that the model is built by the same training data). A simple test class
````TestAccuracyOfModel```` is used to check the accuracy. A shell script 
````run_check_accuracy_on_yarn.sh```` is used to perform this test.


````
./scripts/run_check_accuracy_on_yarn.sh
````

After running the shell script, we find that the model is able to predict diabetes 
with 60% of accuracy against its own training data. Building a machine learning model 
is like tuning a database, which you need to play and tune parameters in order
to build a better model. There is no silver bullet in developing  machine learning
algorithms: you need to understand training data, how to featurize data and how to
tune the building blocks of these algorithms.


````
15/12/30 17:52:10 INFO diabetes.TestAccuracyOfModel: accuracy=0.6015625
````
