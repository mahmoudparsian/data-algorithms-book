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
export SPARK_MASTER=spark://myserver100:7077
#
INPUT=$BOOK_HOME/data/top10data.txt
# Run on a Spark standalone cluster
prog=org.dataalgorithms.chap03.spark.Top10
$SPARK_HOME/bin/spark-submit \
  --class $prog \
  --master $SPARK_MASTER \
  --executor-memory 2G \
  --total-executor-cores 20 \
  $APP_JAR \
  $INPUT
