#!/bin/bash

#
# Description: 
#
#    This script finds Top-N for a given set of (K, V) pairs.
#    The assumption is that all K's are unique.
#
# @author Mahmoud Parsian
#

export JAVA_HOME=/usr/java/jdk7
export SPARK_HOME=/usr/local/spark-1.2.0
export SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly-1.2.0-hadoop2.6.0.jar
export BOOK_HOME=/home/data-algorithms-book
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#
INPUT=$MP/top10data.txt
prog=org.dataalgorithms.chap03.spark.Top10
$SPARK_HOME/bin/spark-submit --class $prog \
    --master yarn-cluster \
    --num-executors 12 \
    --driver-memory 3g \
    --executor-memory 7g \
    --executor-cores 12 \
    --conf "spark.yarn.jar=$SPARK_JAR" \
    $APP_JAR $INPUT 
