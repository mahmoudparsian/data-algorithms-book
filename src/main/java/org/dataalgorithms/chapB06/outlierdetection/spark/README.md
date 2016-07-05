[![Outlier Detection](./outlier.gif)]()

Outlier Detection for Categorical Datasets
==========================================
In Math., an "outlier" is a number in a set of data that 
is either way smaller or way bigger than most of the data.
The main task of "outlier detection" is to find small set 
of data are exceptional compared with the rest of large 
dataset.

The Spark program (````OutlierDetection````) is an 
implementation of a fast and simple outlier detection
method for categorical datasets, called Attribute Value 
Frequency (AVF). The Spark implementation is based on 
the following paper:

* [Fast Parallel Outlier Detection for Categorical Datasets using MapReduce]
  (http://www.eecs.ucf.edu/georgiopoulos/sites/default/files/247.pdf)

The "Attribute Value Frequency (AVF) algorithm is simple and 
faster approach to detect outliers in categorical dataset which 
minimizes the number of scans over the data. It does not create 
more space and more search for combinations of attribute values 
or item sets. An outlier point Xi is de-fined based on the AVF Score" 
(source: [Analysis of outlier detection in categorical dataset]
         (http://pnrsolution.org/Datacenter/Vol3/Issue2/85.pdf)).

For additional references: 
 * [Scalable and Efficient Outlier Detection in Large Distributed Data Sets with Mixed-Type Attributes]
   (http://etd.fcla.edu/CF/CFE0002734/Koufakou_Anna_200908_PhD.pdf)
 
 * [Analysis of Outlier Detection in Categorical Dataset]
   (http://pnrsolution.org/Datacenter/Vol3/Issue2/85.pdf)
 
Spark Algorithm for Outlier Detection
=====================================
We provide two Spark solutions for Outlier Detection:

* [org.dataalgorithms.chapB06.outlierdetection.spark.OutlierDetection (without Lambda Expr.)](./OutlierDetection.java)
* [org.dataalgorithms.chapB06.outlierdetection.spark.OutlierDetectionWithLambda (with Lambda Expr.)](./OutlierDetectionWithLambda.java)

Step    | Description
--------|------------------------------------------------------------------------
Step-1: | Handle input parameters
Step-2: | Create a spark context and then read input and create the first RDD
Step-3: | Perform the map() for each RDD element
Step-4: | Find frequencies of all categorical data (keep categorical-data as String)
Step-5: | Build an associative array to be used for finding AVF Score
Step-6: | Compute AVF Score using the built associative array
Step-7: | Take the lowest K AVF scores (return K detected outliers with minimum AVF Score)
Step-8: | Done & close the spark context
 
Input
===== 
 * Sample input URL: https://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/breast-cancer-wisconsin.data
 * Sample input Data:
 
````
 ...
 1000025,5,1,1,1,2,1,3,1,1,2
 1002945,5,4,4,5,7,10,3,2,1,2
 1015425,3,1,1,1,2,2,3,1,1,2
 1016277,6,8,8,1,3,4,3,7,1,2
 1017023,4,1,1,3,2,1,3,1,1,2
 1017122,8,10,10,8,7,10,9,7,1,4
 ...
````
 
Input Format
============
````
<record-id><,><data1><,><data2><,><data3><,>...
````

Shell Script to Run ````OutlierDetection````
=============================================
````
$ cat run_spark_outlier_detection_yarn.sh 
# define the installation dir for hadoop
export HADOOP_HOME=/Users/mparsian/zmp/zs/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HOME_WARN_SUPPRESS=true

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-1.5.2
#export SPARK_MASTER=spark://localhost:7077
export SPARK_JAR=$BOOK_HOME/lib/spark-assembly-1.5.2-hadoop2.6.0.jar
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
# defines some environment for hadoop
source $HADOOP_CONF_DIR/hadoop-env.sh
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar'  ! -name '*spark-assembly-1.5.2-hadoop2.6.0.jar' `
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#

# define input for Hadoop/HDFS
K=10
INPUT=/outlierdetection/input
#
driver=org.dataalgorithms.chapB06.outlierdetection.spark.OutlierDetection
$SPARK_HOME/bin/spark-submit --class $driver \
	--master yarn-cluster \
	--jars $OTHER_JARS \
    --conf "spark.yarn.jar=$SPARK_JAR" \
	$APP_JAR $K $INPUT
````


Comments and Suggestions
========================
Please email your comments/suggestions to improve the solution: mahmoud.parsian@yahoo.com