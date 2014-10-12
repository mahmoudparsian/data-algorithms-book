#!/bin/bash

# Here, I am assuming that you want to run your spark program in YARN
# This script is a kind of template ...
#   --------------------------------------------------------------------------------
#   1. You have installed the data-algorithms-book  in /home/hadoop/testspark (DAB)
#   2. Hadoop is installed at /usr/local/hadoop-2.5.0 (HADOOP_HOME)
#   3. Hadoop's conf directory is $HADOOP_HOME
#   4. Spark 1.1.0 is installed at /home/hadoop/spark-1.1.0
#   5. And you have built the source code and generated $DAB/dist/data_algorithms_book.jar
#   6. And you have two input parameters identified as P1 and P2
#   7. You need to modify spark-submit parameters accordingly
#   --------------------------------------------------------------------------------
#
export JAVA_HOME=/usr/java/jdk7
export DAB=/home/hadoop/testspark
export SPARK_HOME=/home/hadoop/spark-1.1.0
export THE_SPARK_JAR=$DAB/lib/spark-assembly-1.1.0-hadoop2.5.0.jar
export APP_JAR=$DAB/dist/data_algorithms_book.jar
#
export HADOOP_HOME=/usr/local/hadoop-2.5.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#
# build all other dependent jars in OTHER_JARS
JARS=`find $DAB/lib -name '*.jar'`
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#
P1=<input-parameter-1-for-DRIVER_CLASS_NAME>
P2=<input-parameter-2-for-DRIVER_CLASS_NAME>
DRIVER_CLASS_NAME=<your-driver-class-name>
$SPARK_HOME/bin/spark-submit --class $DRIVER_CLASS_NAME \
    --master yarn-cluster \
    --num-executors 12 \
    --driver-memory 3g \
    --executor-memory 7g \
    --executor-cores 12 \
    --conf "spark.yarn.jar=$THE_SPARK_JAR" \
    --jars $OTHER_JARS \   
    $APP_JAR $P1 $P2
