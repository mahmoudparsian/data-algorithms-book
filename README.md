[Data Algorithms Book](http://shop.oreilly.com/product/0636920033950.do)
======================

[![Data Algorithms Book](./misc/da_small.gif)](http://shop.oreilly.com/product/0636920033950.do)

This repository will host all source code and scripts for
[Data Algorithms Book](http://shop.oreilly.com/product/0636920033950.do).
This book provides a set of MapReduce algrithms, which are implemented using
* Java/MapReduce Hadoop 2.5.0
* Java/Spark 1.0.2 (will upgrade to 1.1.0 in next few days)

Work in Progress...
===================
Please note that this is a work in progress...

![Data Algorithms Book Work In Progress](./misc/work_in_progress2.jpeg)


URL To Data Algorithms Book
===========================
* Title: [Data Algorithms](http://shop.oreilly.com/product/0636920033950.do)
* Author: Mahmoud Parsian
* Publisher: O'Reilly Media


Source Code
===========
* All source code, libraries, and build scripts are posted here
* Shell scripts will be posted for running Spark/Hadoop program (soon!)


Software Used
=============

Software | Version
---------|--------
Java     | JDK7
Hadoop   | 2.5.0
Spark    | 1.0.2
Ant      | 1.9.4


Structure of Repository
=======================

Name          | Description
--------------|------------
README.md     | The file you are reading now
README_lib.md | The file you are reading now (must read before build)
src           | Source files for MapReduce/Hadoop/Spark
lib           | Required jar files
build.xml     | The ant build script
dist          | The ant build's output directory
LICENSE       | License for using this repository
misc          | misc. files for this repository
setenv        | example of how to set your environment variables before building

Structure of src Directory
==========================
![src directory](./misc/source_tree.png)


How To Build using Apache's Ant
===============================
[How To Build by Ant](./misc/how_to_build_with_ant.md)


Sample Builds by Ant
====================
* [Sample Build by Ant for MacBook](./misc/sample_build_mac.txt)
* [Sample Build by Ant for Linux](./misc/sample_build_linux.txt)


How To Run Java/Spark/Hadoop Programs
=====================================
* [How To Run MapReduce/Hadoop Programs](./misc/how_to_run_hadoop_programs.sh)
* [How To Run Java/Spark Programs in YARN](./misc/how_to_run_spark_in_yarn.sh)
* [How To Run Java/Spark Programs in Spark Cluster](./misc/how_to_run_spark_in_spark_cluster.sh)

How To Run Python Programs
==========================

To run python programs just call them with `spark-submit` together with the arguments to the program:

```
spark-submit src/python/chap01/SparkSecondarySort.py local data.csv
```

Questions/Comments
==================
Please send me an email: mahmoud.parsian@yahoo.com

Thank you!
