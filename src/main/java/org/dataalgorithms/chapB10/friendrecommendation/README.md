Friend Recommendation
=====================
The purpose of these MapReduce and Spark programs are to find 
"friends recommnedation" (to offer "people you may know" service). 
We write a [MapReduce program in Hadoop] and a [Spark program] 
that implements a simple “People You May Know” social network 
friendship recommendation algorithm. The main idea is that if 
two people have a lot of mutual friends, then the system should 
recommend that they connect with each other. Our assumption is 
that friendship is bi-directional: if A is a friend of B, then 
B is a friend of A.

Input Format:
=============
````
<person><:><friend1><,><friend2><,>...
````

where ````<friend1>, <friend2>, ...```` are direct friends of ````<person>````

Note that ````<person>, <friend1>, <friend2>, ...```` are userID's of ````java.lang.Long```` data type 

Output Format:
==============
````
<person>  <friend-recommendation-1><,><friend-recommendation-2>...
````

MapReduce/Hadoop Sample Run
===========================
````
#display very small sample input
$HADOOP_HOME/bin/hadoop fs -cat  /friends/input/friends.txt
1:2,3,4,5,6,7
2:1,3,4,5,6
3:1,2
4:1,2
5:1,2
6:1,2
7:1

export INPUT=/friends/input
export OUTPUT=/friends/output
export APP_JAR=/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar
#run the program
export DRIVER=org.dataalgorithms.chapB10.friendrecommendation.mapreduce.FriendRecommendationDriver
$HADOOP_HOME/bin/hadoop jar $APP_JAR $DRIVER 3 $INPUT $OUTPUT

# display output: recommendations per user
$HADOOP_HOME/bin/hadoop fs -cat  /friends/output/parts*
2	7
3	4,5,6
4	3,5,6
5	3,4,6
6	3,4,5
7	2,3,4
````


Spark Sample Run
================

Script
------
````
cat run_recommendation_spark_cluster.sh
# define the installation dir for hadoop
export HADOOP_HOME=/Users/mparsian/zmp/zs/hadoop-2.6.0
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_HOME_WARN_SUPPRESS=true

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home
# java is defined at $JAVA_HOME/bin/java
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-1.5.2
export SPARK_MASTER=spark://localhost:7077
export SPARK_JAR=$BOOK_HOME/lib/spark-assembly-1.5.2-hadoop2.6.0.jar
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
# defines some environment for hadoop
source $HADOOP_CONF_DIR/hadoop-env.sh
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar'`
OTHER_JARS=""
for J in $JARS ; do 
   OTHER_JARS=$J,$OTHER_JARS
done
#
# define input/output for Hadoop/HDFS
MAX_RECOMMENDATIONS=3
INPUT=/friends/input 
OUTPUT=/friends/output
# remove all files under input
$HADOOP_HOME/bin/hadoop fs -rmr $OUTPUT


# remove all files under output
driver=org.dataalgorithms.chapB10.friendrecommendation.spark.SparkFriendRecommendation
$SPARK_HOME/bin/spark-submit --class $driver \
	--master $SPARK_MASTER \
	--jars $OTHER_JARS \
	$APP_JAR $MAX_RECOMMENDATIONS $INPUT $OUTPUT
````

Running Script
--------------
````
# ./run_recommendation_spark_cluster.sh
args[0]: maxNumberOfRecommendations=3
args[1]: <input-path>=/friends/input
args[2]: <output-path>=/friends/output
...
````

Spark Output
------------
````
# display Spark output
hadoop fs -cat /friends/output/part*
(4,[3, 5, 6])
(6,[3, 4, 5])
(3,[4, 5, 6])
(7,[2, 3, 4])
(5,[3, 4, 6])
(2,[7])
````


Comments/Questions/Suggestions
==============================
If you have any questions/comments/suggestions, please let me know: mahmoud.parsian@yahoo.com

````
Thanks,
best regards,
Mahmoud Parsian
```` 
