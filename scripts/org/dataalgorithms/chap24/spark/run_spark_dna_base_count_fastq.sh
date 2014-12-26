#!/bin/bash

#
# Description: 
#
#    This script finds DNA Base Counts.
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
# build all other dependent jars in OTHER_JARS
# jars must be separated by "," (instead of ":")
JARS=`find $BOOK_HOME/lib -name '*.jar'`
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#
echo "OTHER_JARS=$OTHER_JARS"
INPUT=/home/hadoop/testspark/sample.fastq
DRIVER=org.dataalgorithms.chap24.spark.SparkDNABaseCountFASTQ
$SPARK_HOME/bin/spark-submit \
    --class $DRIVER \
    --master $SPARK_MASTER \    
    --executor-memory 2G \
    --total-executor-cores 20 \
    --jars $OTHER_JARS \   
    $APP_JAR $INPUT
