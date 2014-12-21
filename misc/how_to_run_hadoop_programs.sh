#!/bin/bash

# Here, I am assuming that you want to run your MapReduce/Hadoop 
# program in Hadoop's MapReduce environment.
#
# This script is a kind of template ...
#   --------------------------------------------------------------------------------
#   1. You have installed the data-algorithms-book  in /home/mp/data-algorithms-book (BOOK_HOME)
#   2. Hadoop is installed at /usr/local/hadoop-2.6.0 (HADOOP_HOME)
#   3. Hadoop's conf directory is $HADOOP_HOME/etc/hadoop
#   4. You have built the source code and generated $BOOK_HOME/dist/data_algorithms_book.jar
#   5. You have all dependent jars in $BOOK_HOME/lib/*.jar
#   6. And you have two input parameters identified as P1 and P2
#   7. You need to modify directories and parameters accordingly
#   --------------------------------------------------------------------------------
#
export JAVA_HOME=/usr/java/jdk7
# java is defined at $JAVA_HOME/bin/java
export BOOK_HOME=/home/mp/data-algorithms-book
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
export HADOOP_HOME=/usr/local/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=.:$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
#
# copy all jars to HDFS's /lib/ directory
# It is assumed that all jars in /lib/*.jar will be put 
# into Hadoop's Distributed cache by the DRIVER_CLASS_NAME
# for details see org.dataalgorithms.util.HadoopUtil class
hadoop fs -mkdir /lib
hadoop fs -copyFromLocal $APP_JAR  /lib/
hadoop fs -copyFromLocal $BOOK_HOME/lib/*.jar  /lib/
#
P1=<input-parameter-1-for-DRIVER_CLASS_NAME>
P2=<input-parameter-2-for-DRIVER_CLASS_NAME>
DRIVER_CLASS_NAME=<your-driver-class-name>
$HADOOP_HOME/bin/hadoop  jar $APP_JAR $DRIVER_CLASS_NAME $P1 $P2
