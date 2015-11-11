# This is an example to set 3 environment variables 
# for compiling/running MapReduce and Spark programs

# IMPORTANT NOTE: You should update your script 
# accordingly, where ever you see "mparsian", 
# please change it to your installed directories.


#
#set Java as jdk7
#

# macbook:
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home

# linux:
# export JAVA_HOME=/usr/java/jdk7

echo "JAVA_HOME=$JAVA_HOME"
#

#
# set ant 
#
export ANT_HOME=/Users/mparsian/zmp/zs/apache-ant-1.9.4
echo "ANT_HOME=$ANT_HOME"
#

#
# set your spark and hadoop environments
#
export SPARK_HOME=/usr/local/spark-1.5.2
export HADOOP_HOME=/Users/mparsian/zmp/zs/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# set PATH
export PATH=$JAVA_HOME/bin:$ANT_HOME/bin:$HADOOP_HOME/bin:$PATH
echo "PATH=$PATH"
#
BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR
jars=`find $BOOK_HOME/lib -name '*.jar'`
for j in $jars ; do
	CLASSPATH=$CLASSPATH:$j
done
#
CLASSPATH=$CLASSPATH:$BOOK_HOME/dist/data_algorithms_book.jar
#
export CLASSPATH=$CLASSPATH
export HADOOP_CLASSPATH=$CLASSPATH
