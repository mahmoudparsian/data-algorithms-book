#!/bin/bash
export JAVA_HOME=/usr/java/jdk7
export MP=/home/hadoop/testspark
export SPARK_HOME=/home/hadoop/spark-1.1.0
export THE_SPARK_JAR=$MP/lib/spark-assembly-1.1.0-hadoop2.5.0.jar
export APP_JAR=$MP/dist/data_algorithms_book.jar
#
export HADOOP_HOME=/usr/local/hadoop/hadoop-2.5.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#
# build all other dependent jars in OTHER_JARS
JARS=`find $MP/lib -name '*.jar'`
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
