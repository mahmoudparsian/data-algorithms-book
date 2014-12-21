#!/bin/bash

#
# Description: 
#
#    This script finds Top-N for a given set of (K, V) pairs.
#    This is accomplished in two basic steps:
#
#         Step-1:  aggregate keys: generate unique keys and aggregate values
#                  If your input have (K,V1), (K,V2), and (K, V3), then
#                  this phase will generate (K, V) where V = V1+V2+V3.
#
#         Step-2: find top-N for unique keys using takeOrdered() function
#
# @author Mahmoud Parsian
#

export JAVA_HOME=/usr/java/jdk7
export SPARK_HOME=/usr/local/spark-1.2.0
export SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly-1.1.1-hadoop2.6.0.jar
export BOOK_HOME=/home/data-algorithms-book
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#
INPUT=/top10/top10input
TOPN=4
prog=org.dataalgorithms.chap03.spark.Top10UsingTakeOrdered
$SPARK_HOME/bin/spark-submit 
	--class $prog \
    --master yarn-cluster \
    --num-executors 12 \
    --driver-memory 3g \
    --executor-memory 7g \
    --executor-cores 12 \
    --conf "spark.yarn.jar=$SPARK_JAR" \
    $APP_JAR $INPUT $TOPN 
