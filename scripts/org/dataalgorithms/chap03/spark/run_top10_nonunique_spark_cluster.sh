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
#         Step-2: find top-N for unique keys
#
# @author Mahmoud Parsian
#

export JAVA_HOME=/usr/java/jdk7
export SPARK_HOME=/usr/local/spark-1.2.0
export SPARK_JAR=$SPARK_HOME/assembly/target/scala-2.10/spark-assembly-1.2.0-hadoop2.6.0.jar
export SPARK_MASTER=spark://myserver100:7077
export BOOK_HOME=/home/data-algorithms-book
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
INPUT=$BOOK_HOME/data/top10data.txt
topN=2
prog=org.dataalgorithms.chap03.spark.Top10NonUnique
# Run on a Spark standalone cluster
$SPARK_HOME/bin/spark-submit \
  --class $prog \
  --master $SPARK_MASTER \
  --executor-memory 2G \
  --total-executor-cores 20 \
  $APP_JAR \
  $INPUT $topN
