Submit a Spark job to YARN from Java Code
=========================================
In this post I will show you how to submit a Spark job from Java code. 
Typically, we submit Spark jobs to "Spark Cluster" and Hadoop/YARN  by 
using ````$SPARK_HOME/bin/spark-submit````  shell script.  Submitting 
Spark job from a shell script limits programmers when they want to submit 
Spark jobs from Java code (such as Java servlets or other Java code such 
as REST servers).

Spark Version
=============

````
spark-1.6.1
````

Use YARN's Client Class
=======================
Below is a complete Java code, which submits a Spark job to YARN from Java 
code (no shell scripting  is required). The main class used for submitting 
a Spark job to YARN is the ```org.apache.spark.deploy.yarn.Client```` class.


Complete Java Client Program
============================
[SubmitSparkJobToYARNFromJavaCode](https://raw.githubusercontent.com/mahmoudparsian/data-algorithms-book/master/src/main/java/org/dataalgorithms/client/SubmitSparkJobToYARNFromJavaCode.java)


Running Complete Java Client Program
====================================

Now, we can submit Spark job from Java code:
````
export LIB_DIR=/Users/mparsian/zmp/github/data-algorithms-book/lib
$ java  org.dataalgorithms.chapB13.client.SubmitSparkJobToYARNFromJavaCode  $LIB_DIR
````

Note
====
Before running your Java code, make sure that the HDFS output directory does not exist:
````
hadoop fs -rm -R /friends/output
````

Thank you!
==========

````
best regards,
Mahmoud Parsian
````

[![Data Algorithms Book](https://github.com/mahmoudparsian/data-algorithms-book/blob/master/misc/data_algorithms_image.jpg)](http://shop.oreilly.com/product/0636920033950.do) 
