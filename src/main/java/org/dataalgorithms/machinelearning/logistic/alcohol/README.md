Machine Learning Algorithms using Spark: Logistic Regression with K Classifications
===================================================================================
The purpose of this package (solutions expressed in Java 
and Spark) is to show how to implement basic machine learning 
algorithms such as Logistic Regression in Spark and Spark's 
MLlib library.  Spark's MLlib offers a suite of machine learning 
libraries for Logistic Regression and other machine learning
algorithms. 

This example focuses on detecting student alcohol using 14  features
and 5 classifications. For more information on this example, please 
refer to https://github.com/KoolCards/StudentAlcohol-ml


Student Alcohol Detection
=======================
These Spark programs detect student alcohol using Logistic Regression model 

* org.dataalgorithms.machinelearning.logistic.alcohol.StudentAlcoholDetectionBuildModel

The class ````StudentAlcoholDetectionBuildModel```` builds the model from the given training data

* org.dataalgorithms.machinelearning.logistic.alcohol.StudentAlcoholDetection

This is the driver class, which uses the built model to classify new queried data


Training Data
=============

* 14 Feature columns: 1,2,3,4,5,7,8,9,12,14,17,18,19,23

* Features:
````
  Sex
  Age
  Urban or Rural Home
  Mother's Education
  Father's Education
  Study Time
  Failed Classes
  Supplementary Education
  Extracuriculars
  Higher Education
  Family Relationships
  Free Time
  Activities with Friends
  Days Absent
````

* Classification (1,2,3,4,5):
````
  Weekly Alcohol Consumption = 1 (very low)
  Weekly Alcohol Consumption = 2 (low)
  Weekly Alcohol Consumption = 3 (moderate)
  Weekly Alcohol Consumption = 4 (high)
  Weekly Alcohol Consumption = 5 (very high)
````


* Classification column: 20 

* Sample Training data
````
# head $BOOK_HOME/data/student_alcohol_training_data.txt
1,18,0,4,4,2,2,0,1,0,0,0,1,1,0,0,4,3,4,1,1,3,6,5,6,6
1,17,0,1,1,1,2,0,0,1,0,0,0,1,1,0,5,3,3,1,1,3,4,5,5,6
1,15,0,1,1,1,2,3,1,0,1,0,1,1,1,0,4,3,2,2,3,3,10,7,8,10
1,15,0,4,2,1,3,0,0,1,1,1,1,1,1,1,3,2,2,1,1,5,2,15,14,15
1,16,0,3,3,1,2,0,0,1,1,0,1,1,0,0,4,3,2,1,2,5,4,6,10,10
0,16,0,4,3,1,2,0,0,1,1,1,1,1,1,0,5,4,2,1,2,5,10,15,15,15
0,16,0,2,2,1,2,0,0,0,0,0,1,1,1,0,4,4,4,1,1,3,0,12,12,11
1,17,0,4,4,2,2,0,1,1,0,0,1,1,0,0,4,1,4,1,1,1,6,6,5,6
0,15,0,3,2,1,2,0,0,1,1,0,1,1,1,0,4,2,2,1,1,1,0,16,18,19
0,15,0,3,4,1,2,0,0,1,1,1,1,1,1,0,5,5,1,1,1,5,0,14,15,15
````

Build LogisticRegressionModel
===========
The class ````StudentAlcoholDetectionBuildModel```` reads the training data and
builds a ````LogisticRegressionModel```` and saves it. The following script is 
used to build the model:

````
$cat run_student_alcohol_build_model.sh
set -x
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.0.2-bin-hadoop2.6
export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar'`
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#
export training="file://$BOOK_HOME/data/student_alcohol_training_data.txt"
export model="file://$BOOK_HOME/model"
#
driver=org.dataalgorithms.machinelearning.logistic.alcohol.StudentAlcoholDetectionBuildModel
$SPARK_HOME/bin/spark-submit --class $driver --verbose \
	--master local \
	--jars $OTHER_JARS \
	$APP_JAR $training $model
````



Using LogisticRegressionModel
=============================
The following script is used to read the built model and new query data:
````
$ cat ./run_student_alcohol_detect.sh
set -x
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.0.2-bin-hadoop2.6
export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar'`
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#
export query="file://$BOOK_HOME/data/student_alcohol_training_data.txt"
export model="file://$BOOK_HOME/model"
# 
driver=org.dataalgorithms.machinelearning.logistic.alcohol.StudentAlcoholDetection
$SPARK_HOME/bin/spark-submit --class $driver --verbose \
	--master local \
	--jars $OTHER_JARS \
	$APP_JAR $query $model

````