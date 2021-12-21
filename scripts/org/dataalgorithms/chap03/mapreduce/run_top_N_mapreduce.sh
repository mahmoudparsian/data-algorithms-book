#!/bin/bash

#
# Description: 
#
#    This script finds Top-N (for given N > 0) in two MapReduce phases:
#
#         phase 1: aggregate keys: generate unique keys and aggregate values
#                  If your input have (K,V1), (K,V2), and (K, V3), then
#                  this phase will generate (K, V) where V = V1+V2+V3.
#
#         phase 2: find top-N for unique keys
#
# @author Mahmoud Parsian
#

export JAVA_HOME=/usr/java/jdk7
export HADOOP_HOME=/usr/local/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export BOOK_HOME=/home/data-algorithms-book
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH
#
# phase 1: aggregate keys
#
INPUT=/kv/input
OUTPUT=/kv/output
hadoop fs -rmr $OUTPUT
AGGREGATOR=org.dataalgorithms.chap03.mapreduce.AggregateByKeyDriver
hadoop jar $APP_JAR  $AGGREGATOR $INPUT $OUTPUT
#
# Phase 2: Run top N
#
N=5
TopN_INPUT=/kv/output
TopN_OUTPUT=/kv/final
hadoop fs -rmr $TopN_OUTPUT
TopN=org.dataalgorithms.chap03.mapreduce.TopNDriver
hadoop jar $APP_JAR $TopN $N $TopN_INPUT $TopN_OUTPUT
