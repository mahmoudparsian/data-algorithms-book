Per Key Average
===============
The purpose of the Spark program (PerKeyAverage.java) is to find 
"per key average" for all keys. Here we provide two classes:

* ````org.dataalgorithms.chapB03.perkeyaverage.spark.PerKeyAverage```` (without using Lambda Expressions)
* ````org.dataalgorithms.chapB03.perkeyaverage.sparkwithlambda.PerKeyAverage```` (with using Lambda Expressions)

Input Format:
=============
Each record has the following format:
````
<key-as-string><:><value-as-double>
````

Output Format:
==============
````
<key>  <average-per-key>
````

Shell Script
============
````
cat ./run_perkeyaverage_spark.sh
#!/bin/bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-1.5.2
export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
prog=org.dataalgorithms.chap03.perkeyaverage.spark.PerKeyAverage
$SPARK_HOME/bin/spark-submit  --class $prog --master $SPARK_MASTER $APP_JAR
````

Sample Run and Output
=====================
````
./run_perkeyaverage_spark.sh
pandas:10.0
zebra:4.0
duck:5.0
````


Comments/Questions/Suggestions
==============================
If you have any questions/comments/suggestions, please let me know: mahmoud.parsian@yahoo.com

````
Thanks,
best regards,
Mahmoud Parsian
```` 
