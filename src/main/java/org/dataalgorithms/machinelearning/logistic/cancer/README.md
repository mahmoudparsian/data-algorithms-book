Machine Learning Algorithms using Spark: Logistic Regression
============================================================
The purpose of this package (solutions expressed in Java 
and Spark) is to show how to implement basic machine learning 
algorithms such as Logistic Regression in Spark and Spark's MLlib 
library.  Spark's MLlib offers a suite of machine learning 
libraries for Logistic Regression and other machine learning
algorithms. This example focuses on detecting breast cancers 
using 30 patient features.


Breast Cancer Detection
=======================
These Spark programs detect breast cancer using Logistic Regression model 

* org.dataalgorithms.machinelearning.logistic.BreastCancerDetectionBuildModel

The class ````BreastCancerDetectionBuildModel```` builds the model from the given training data

* org.dataalgorithms.machinelearning.logistic.BreastCancerDetection

This is the driver class, which uses the built model to classify new queried data


Training Data
=============
To build a ````LogisticRegressionModel````, we need a training data set.
The training data set is for breast cancer patients and each record has
the following input format:

````
<patient-id><,><tag><,><feature-1><,><feature-2>...<feature-30>
````

where tag is in {B, M} (this is the classification of the training data set)

 *  M = Malignant
 *  B = Benign

The training data is located at ````resources/breast-cancer-wisconsin-wdbc-data.txt````

Build Model
===========
The class ````BreastCancerDetectionBuildModel```` reads the training data and
builds a ````LogisticRegressionModel```` and saves it in HDFS.

To build our logistic regression model, we use a shell script: ````run_build_model_on_yarn.sh````.
After running the shell script, the model is saved in HDFS as a parquet file:

````
# ./scripts/run_cancer_detection_on_yarn.sh

# hadoop fs -ls -R /breastcancer
drwxr-xr-x   - mparsian ...          0 2015-12-26 19:30 /breastcancer/model
drwxr-xr-x   - mparsian ...          0 2015-12-26 19:30 /breastcancer/model/data
-rw-r--r--   1 mparsian ...          0 2015-12-26 19:30 /breastcancer/model/data/_SUCCESS
-rw-r--r--   1 mparsian ...        939 2015-12-26 19:30 /breastcancer/model/data/_common_metadata
-rw-r--r--   1 mparsian ...       1660 2015-12-26 19:30 /breastcancer/model/data/_metadata
-rw-r--r--   1 mparsian ...       1903 2015-12-26 19:30 /breastcancer/model/data/part-r-00000-73fc5366-4219-40b6-a5f9-7e0d6a9932b8.gz.parquet
drwxr-xr-x   - mparsian ...          0 2015-12-26 19:30 /breastcancer/model/metadata
-rw-r--r--   1 mparsian ...          0 2015-12-26 19:30 /breastcancer/model/metadata/_SUCCESS
-rw-r--r--   1 mparsian ...        122 2015-12-26 19:30 /breastcancer/model/metadata/part-00000
drwxr-xr-x   - mparsian ...          0 2015-12-26 19:34 /breastcancer/query
-rw-r--r--   1 mparsian ...       1723 2015-12-26 19:34 /breastcancer/query/query-data.txt
drwxr-xr-x   - mparsian ...          0 2015-11-02 16:42 /breastcancer/training
-rw-r--r--   1 mparsian ...     124103 2015-11-02 16:42 /breastcancer/training/breast-cancer-wisconsin-wdbc-data.txt

# hadoop fs -cat /breastcancer/model/metadata/part-00000
{
 "class":"org.apache.spark.mllib.classification.LogisticRegressionModel",
 "version":"1.0",
 "numFeatures":30,
 "numClasses":2
}
````

Note that the data is stored in parquet format (which is a binary data).
To view the contents, we can convert it to CSV format and view it:

````
# hadoop fs -copyToLocal /breastcancer/model/data/part-r-00000-73fc5366-4219-40b6-a5f9-7e0d6a9932b8.gz.parquet .

# java org.dataalgorithms.parquet.TestConvertParquetToCSV ~/zmp/bin/part-r-00000-73fc5366-4219-40b6-a5f9-7e0d6a9932b8.gz.parquet part.csv
2015-12-26 19:59:56 INFO  TestConvertParquetToCSV:20 - parquetFile=/Users/mparsian/zmp/bin/part-r-00000-73fc5366-4219-40b6-a5f9-7e0d6a9932b8.gz.parquet
2015-12-26 19:59:56 INFO  TestConvertParquetToCSV:21 - csvOutputFile=part.csv
Dec 26, 2015 7:59:56 PM org.apache.parquet.Log info
INFO: Converting part-r-00000-73fc5366-4219-40b6-a5f9-7e0d6a9932b8.gz.parquet to part.csv
2015-12-26 19:59:58 INFO  CodecPool:179 - Got brand-new decompressor [.gz]
2015-12-26 19:59:58 INFO  TestConvertParquetToCSV:25 - estimatedTime (millis)=1403
Dec 26, 2015 7:59:58 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Dec 26, 2015 7:59:58 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: reading another 1 footers
Dec 26, 2015 7:59:58 PM INFO: org.apache.parquet.hadoop.ParquetFileReader: Initiating action with parallelism: 5
Dec 26, 2015 7:59:58 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: RecordReader initialized will read a total of 1 records.
Dec 26, 2015 7:59:58 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: at row 0. reading next block
Dec 26, 2015 7:59:58 PM INFO: org.apache.parquet.hadoop.InternalParquetRecordReader: block read in memory in 88 ms. row count = 1

# cat part.csv 
type: 1
values
  array: 21.475005760229195
  array: 39.67403165375954
  array: 131.28277977399267
  array: 169.7094691934828
  array: 0.22456462187692777
  array: 0.07313044337404251
  array: -0.1286891118367356
  array: -0.06506153759421998
  array: 0.42455929438001583
  array: 0.16789768818930118
  array: 0.12628124581314404
  array: 3.129146423064525
  array: 0.7026820469046879
  array: -43.629299487764634
  array: 0.019157363943287962
  array: 0.030484620012944892
  array: 0.029917814469143492
  array: 0.01383575896267864
  array: 0.05252915327511932
  array: 0.008297541747238058
  array: 20.64272307503251
  array: 50.64189979036252
  array: 125.58421836712478
  array: -165.84380930999217
  array: 0.2940585511038301
  array: 0.08478843766328406
  array: -0.16502037909669698
  array: -0.026437123744129185
  array: 0.0
  array: 0.0
|0.0|0.5
````

Using LogisticRegressionModel
=============================
To use the built model, we have prepared 8 query records:

````
# cat resources/query-data.txt 
842302,17.99,10.38,122.8,1001,0.1184,0.2776,0.3001,0.1471,0.2419,0.07871,1.095,0.9053,8.589,153.4,0.006399,0.04904,0.05373,0.01587,0.03003,0.006193,25.38,17.33,184.6,2019,0.1622,0.6656,0.7119,0.2654,0.4601,0.1189
842517,20.57,17.77,132.9,1326,0.08474,0.07864,0.0869,0.07017,0.1812,0.05667,0.5435,0.7339,3.398,74.08,0.005225,0.01308,0.0186,0.0134,0.01389,0.003532,24.99,23.41,158.8,1956,0.1238,0.1866,0.2416,0.186,0.275,0.08902
84300903,19.69,21.25,130,1203,0.1096,0.1599,0.1974,0.1279,0.2069,0.05999,0.7456,0.7869,4.585,94.03,0.00615,0.04006,0.03832,0.02058,0.0225,0.004571,23.57,25.53,152.5,1709,0.1444,0.4245,0.4504,0.243,0.3613,0.08758
84348301,11.42,20.38,77.58,386.1,0.1425,0.2839,0.2414,0.1052,0.2597,0.09744,0.4956,1.156,3.445,27.23,0.00911,0.07458,0.05661,0.01867,0.05963,0.009208,14.91,26.5,98.87,567.7,0.2098,0.8663,0.6869,0.2575,0.6638,0.173
8510426,13.54,14.36,87.46,566.3,0.09779,0.08129,0.06664,0.04781,0.1885,0.05766,0.2699,0.7886,2.058,23.56,0.008462,0.0146,0.02387,0.01315,0.0198,0.0023,15.11,19.26,99.7,711.2,0.144,0.1773,0.239,0.1288,0.2977,0.07259
8510653,13.08,15.71,85.63,520,0.1075,0.127,0.04568,0.0311,0.1967,0.06811,0.1852,0.7477,1.383,14.67,0.004097,0.01898,0.01698,0.00649,0.01678,0.002425,14.5,20.49,96.09,630.5,0.1312,0.2776,0.189,0.07283,0.3184,0.08183
8510824,9.504,12.44,60.34,273.9,0.1024,0.06492,0.02956,0.02076,0.1815,0.06905,0.2773,0.9768,1.909,15.7,0.009606,0.01432,0.01985,0.01421,0.02027,0.002968,10.23,15.66,65.13,314.9,0.1324,0.1148,0.08867,0.06227,0.245,0.07773
857155,12.05,14.63,78.04,449.3,0.1031,0.09092,0.06592,0.02749,0.1675,0.06043,0.2636,0.7294,1.848,19.87,0.005488,0.01427,0.02322,0.00566,0.01428,0.002422,13.76,20.7,89.88,582.6,0.1494,0.2156,0.305,0.06548,0.2747,0.08301
````

where the first 4 records are Malignant and the last 4 records are Benign;
each record has the following format:

````
<patient-id><,><feature-1><,><feature-2>...<feature-30>
````

To run our query data against the built model, we use a shell script: ````run_cancer_detection_on_yarn.sh````

````
# ./scripts/run_cancer_detection_on_yarn.sh
=== begin DEBUG-by-MP ===
DEBUG-by-MP: parameter p=org.apache.spark.deploy.SparkSubmit
DEBUG-by-MP: parameter p=--class
DEBUG-by-MP: parameter p=org.dataalgorithms.machinelearning.logistic.cancer.BreastCancerDetection
DEBUG-by-MP: parameter p=--master
DEBUG-by-MP: parameter p=yarn-cluster
DEBUG-by-MP: parameter p=--jars
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/lib/spring-context-3.0.7.RELEASE.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/log4j-1.2.17.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/junit-4.12-beta-2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jsch-0.1.42.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/JeraAntTasks.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jedis-2.5.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jblas-1.2.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/hamcrest-all-1.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/guava-18.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math3-3.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math-2.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-logging-1.1.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang3-3.4.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang-2.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-io-2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-httpclient-3.0.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-daemon-1.0.5.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-configuration-1.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-collections-3.2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-cli-1.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/cloud9-1.3.2.jar,
DEBUG-by-MP: parameter p=--conf
DEBUG-by-MP: parameter p=spark.yarn.jar=/Users/mparsian/zmp/github/data-algorithms-book/lib/spark-assembly-1.5.2-hadoop2.5.0.jar
DEBUG-by-MP: parameter p=/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar
DEBUG-by-MP: parameter p=/breastcancer/query
DEBUG-by-MP: parameter p=/breastcancer/model
=== done DEBUG-by-MP ===
````

Output from BreastCancerDetection Run
=====================================
As you can see from the output, the built model predicted all 8 query records correctly!!!
The following output is from the Spark logs.
 
````
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=842302
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=0.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=842517
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=0.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=84300903
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=0.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=84348301
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=0.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=8510426
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=1.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=8510653
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=1.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=8510824
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=1.0
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: query: patientID=857155
15/12/26 19:35:33 INFO cancer.BreastCancerDetection: prediction=1.0
````

