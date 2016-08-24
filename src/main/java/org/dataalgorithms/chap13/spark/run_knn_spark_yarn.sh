#!/bin/bash
/bin/date
# define the installation dir for hadoop
export HADOOP_HOME=/Users/mparsian/zmp/zs/hadoop-2.6.3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HOME_WARN_SUPPRESS=true
# defines some environment for hadoop
source $HADOOP_CONF_DIR/hadoop-env.sh
#
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.0.0-bin-hadoop2.6
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
#
k=4
d=2
R="hdfs:///dataalgorithms/chap13/spark/resources/R.txt"
S="hdfs:///dataalgorithms/chap13/spark/resources/S.txt"
OUTPUT="hdfs:///dataalgorithms/chap13/spark//output"
prog=org.dataalgorithms.chap13.spark.kNN
#
$SPARK_HOME/bin/spark-submit --class $prog \
    --master yarn \
    --num-executors 2 \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    --jars $OTHER_JARS \
    $APP_JAR $k $d $R $S $OUTPUT

