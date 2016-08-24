#!/bin/bash
/bin/date
#
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.0.0-bin-hadoop2.6
export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
#
k=4
d=2
R="file://$BOOK_HOME/src/main/java/org/dataalgorithms/chap13/spark/resources/R.txt"
S="file://$BOOK_HOME/src/main/java/org/dataalgorithms/chap13/spark/resources/S.txt"
OUTPUT="file://$BOOK_HOME/src/main/java/org/dataalgorithms/chap13/spark/output"
prog=org.dataalgorithms.chap13.spark.kNN
#
$SPARK_HOME/bin/spark-submit --class $prog \
    --master local \
    --num-executors 2 \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    $APP_JAR $k $d $R $S $OUTPUT
