#---------------------------------------------------
# This is an example to set environment variables 
# for compiling/running MapReduce and Spark programs
#---------------------------------------------------
#
# IMPORTANT NOTE: You should update your script 
# accordingly, where ever you see "mparsian", 
# please change it to your installed directories.
#---------------------------------------------------


#-----------------
# set Java as jdk8
#-----------------

# macbook:
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home

# linux:
# export JAVA_HOME=/usr/java/jdk8

echo "JAVA_HOME=$JAVA_HOME"
#

#--------
# set ant 
#--------
export ANT_HOME=/Users/mparsian/zmp/zs/apache-ant-1.9.4
echo "ANT_HOME=$ANT_HOME"
#
export SCALA_HOME=/Users/mparsian/scala-2.11.6
#export SCALA_HOME=/Users/mparsian/scala-2.11.8
echo "SCALA_HOME=$SCALA_HOME"

#---------------------------------------
# set your spark and hadoop environments
#---------------------------------------
export SPARK_HOME=/Users/mparsian/spark-2.0.0-bin-hadoop2.6
export HADOOP_HOME=/Users/mparsian/zmp/zs/hadoop-2.6.3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

#---------
# set PATH
#---------
export PATH=$SCALA_HOME/bin:$JAVA_HOME/bin:$ANT_HOME/bin:$HADOOP_HOME/bin:$PATH
echo "PATH=$PATH"
#
BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR
#
jars=`find $BOOK_HOME/lib -name '*.jar'`
for j in $jars ; do
	CLASSPATH=$CLASSPATH:$j
done
#
#
jars=`find $HADOOP_HOME/ -name '*.jar'`
for j in $jars ; do
	CLASSPATH=$j:$CLASSPATH
done
#
#
jars=`find $SCALA_HOME/lib -name '*.jar'`
for j in $jars ; do
	CLASSPATH=$j:$CLASSPATH
done
#
CLASSPATH=$BOOK_HOME/dist/data_algorithms_book.jar:$CLASSPATH
CLASSPATH=$HADOOP_CONF_DIR:$CLASSPATH
#
#-------------------
# finalize CLASSPATH
#-------------------
export CLASSPATH=$CLASSPATH
export HADOOP_CLASSPATH=$CLASSPATH
