#!/bin/bash
export MP=/home/hadoop/testspark
export THE_SPARK_JAR=$MP/spark-assembly-1.1.1-hadoop2.5.0.jar
export JAVA_HOME=/usr/java/jdk7
export BOOK_HOME=/home/data-algorithms-book
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
export SPARK_HOME=/home/hadoop/spark-1.1.1
#
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.5.0
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
    --conf "spark.yarn.jar=$THE_SPARK_JAR" \
    $APP_JAR $INPUT $TOPN 
