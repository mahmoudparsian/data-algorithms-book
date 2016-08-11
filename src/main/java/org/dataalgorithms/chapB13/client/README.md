Submit a Spark job to YARN/Cluster from Java Code
=================================================
In this post I will show you how to submit a Spark job from Java code. 
Typically, we submit Spark jobs to "Spark Cluster" and Hadoop/YARN  by 
using the ````$SPARK_HOME/bin/spark-submit````  shell script.  Submitting 
Spark job from a shell script limits programmers when they want to submit 
Spark jobs from Java code (such as Java servlets or other Java code such 
as REST servers).

Spark Version
=============

````
spark-2.0.0
````

Client Package ````org.dataalgorithms.chapB13.client````
========================================================
This package contains the following  programs, which submit
Spark jobs to Spark Cluster or to Hadoop/YARN.


Program/File                                |  Description                                                    |
------------------------------------------- | --------------------------------------------------------------- | 
[ConfigurationManager.java](./ConfigurationManager.java)                   |  Creates an Hadoop ````Configuration```` object                 | 
[SubmitSparkJobToClusterFromJavaCode.java](./SubmitSparkJobToClusterFromJavaCode.java)    |  Submits a Spark job to Spark cluster from Java code            |
[SubmitSparkJobToYARNFromJavaCode.java](./SubmitSparkJobToYARNFromJavaCode.java)      |  Submits a Spark job to Hadoop/YARN from Java code              |
[SubmitSparkPiToClusterFromJavaCode.java](./SubmitSparkPiToClusterFromJavaCode.java)     |  Submits a SparkPi to Spark cluster from Java code              |         
[SubmitSparkPiToClusterFromJavaCode.log](./SubmitSparkPiToClusterFromJavaCode.log)     |  log file                                                       |
[SubmitSparkPiToYARNFromJavaCode.java](./SubmitSparkPiToYARNFromJavaCode.java)       |  Submits a SparkPi to Hadoop/YARN from Java code                |
[SubmitSparkPiToYARNFromJavaCode.log](./SubmitSparkPiToYARNFromJavaCode.log)        |  log file                                                       |
[SubmitSparkPiToYARNFromJavaCode.stderr.html](./SubmitSparkPiToYARNFromJavaCode.stderr.html) |  stderr file                                                    |
[SubmitSparkPiToYARNFromJavaCode.stdout.html](./SubmitSparkPiToYARNFromJavaCode.stdout.html) |  stdout file                                                    |


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

[![Data Algorithms Book](https://github.com/mahmoudparsian/data-algorithms-book/blob/master/misc/large-image.jpg)](http://shop.oreilly.com/product/0636920033950.do) 
