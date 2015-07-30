[Data Algorithms Book](http://shop.oreilly.com/product/0636920033950.do)
======================
Data Algorithms: Recipes for Scaling up with Hadoop and Spark


[Added a New Bonus Chapter: Rank Product](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus/rankproduct)
=========================================
 

[Production Version is Available NOW!](http://shop.oreilly.com/product/0636920033950.do)
======================================
[![Data Algorithms Book](./misc/da_book3.jpeg)](http://shop.oreilly.com/product/0636920033950.do)
 
Going to Production
===================
I am so excited to report that the "Data Algorithms" will be going to production this month!
This mean that a HARD COPY will be available very soon!
  

Author Book Signing @Spark Summit 2015
=====================================
O'Reilly author book signings for 
[Data Algorithms](http://shop.oreilly.com/product/0636920033950.do) 
will be held in the O'Reilly booth 
[#SparkSummit2015](https://twitter.com/hashtag/sparksummit2015) 
on Tuesday, June 16, 2015 at 2:30pm.

Thank YOU!
==========
I had an honor an privilege of signing my book at #SparkSummit 2015. I want to say big thank you 
to all who waited in the line to get a signed copy. What a fantastic group of engineers and 
data scientists! I was amazed a and learned a lot from this group. Please note that the signed copy
has only the first 5 chapters. The [full PDF](http://shop.oreilly.com/product/0636920033950.do) is 
available and the production copy will be ready by July 2015 (I hope!).

[Author Book Signing](http://strataconf.com/big-data-conference-ca-2015/public/content/author-signings)
====================
Author book signings for ("Data Algorithms") will be held in the O'Reilly booth on Thursday, Feb. 19, 2015.
Complimentary copies of books will be provided for the first 25 attendees. 

[Bonus Chapters](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus)
================
The story of bonus chapters: originally, I had about 60+ chapters for the book (which would 
have been over 1000+ pages -- too much!!!). To keep it short, sweet, and focused, I put 31 
chapters in the book and then put the remaining chapters (as bonus chapters) in here.  I have 
started adding bonus chapters. Already have added the following bonus chapters:

Bonus chapter                                                                                                                                          | Description
-------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------
[Anagram](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus/anagram)                           | Anagram detection in Spark and MapReduce/Hadoop
[Cartesian](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus/cartesian)                       | How to perform "cartesian" operation in Spark
[Friend Recommendation](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus/friendrecommendation) | Friends Recommnedation algorithms in Spark and MapReduce/Hadoop 
[Log Query](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus/logquery)                         | Basic log query implementation  in Spark and MapReduce/Hadoop  
[Word Count](https://github.com/mahmoudparsian/data-algorithms-book/tree/master/src/main/java/org/dataalgorithms/bonus/wordcount)                       | Hello World! of Spark and MapReduce/Hadoop   


Repository
==========
This repository will host all source code and scripts for
[Data Algorithms Book](http://shop.oreilly.com/product/0636920033950.do).
This book provides a set of distributed MapReduce algrithms, which are implemented using
* Java (JDK7)
* Spark 1.4.0
* MapReduce/Hadoop 2.6.0

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
* All [source code](./src), libraries, and build scripts are posted here
* [Shell scripts](./scripts) are posted for running Spark and Mapreduce/Hadoop programs (in progress...)


Software Used
=============

Software | Version
---------|--------
Java     | JDK7
Hadoop   | 2.6.0
Spark    | 1.4.0
Ant      | 1.9.4


Structure of Repository
=======================

Name          | Description
--------------|------------
README.md     | The file you are reading now
README_lib.md | Must read before you build with Ant
src           | Source files for MapReduce/Hadoop/Spark
scripts       | Shell scripts to run MapReduce/Hadoop and Spark pograms
lib           | Required jar files for compiling source code
build.xml     | The ant build script
dist          | The ant build's output directory (creates a single JAR file)
LICENSE       | License for using this repository (Apache License, Version 2.0)
misc          | misc. files for this repository
setenv        | example of how to set your environment variables before building
data          | sample data files (such as FASTQ and FASTA) for basic testing purposes

Structure of src Directory
==========================
![src directory](./misc/source_tree.png)

Also, each chapter has two sub folders:
```
org.dataalgorithms.chapNN.spark      (for Spark programs)
org.dataalgorithms.chapNN.mapreduce  (for Mapreduce/Hadoop programs)

where NN = 00, 01, ..., 31
```

How To Build using Apache's Ant
===============================
[How To Build by Ant](./misc/how_to_build_with_ant.md)


Sample Builds by Ant
====================
* [Sample Build by Ant for MacBook](./misc/sample_build_mac.txt)
* [Sample Build by Ant for Linux](./misc/sample_build_linux.txt)


How To Run Spark/Hadoop Programs
================================
* [How To Run MapReduce/Hadoop Programs](./misc/how_to_run_hadoop_programs.sh)
* [How To Run Java/Spark Programs in YARN](./misc/how_to_run_spark_in_yarn.sh)
* [How To Run Java/Spark Programs in Spark Cluster](./misc/how_to_run_spark_in_spark_cluster.sh)


[Submit a Spark job to YARN from Java Code](./misc/how-to-submit-spark-job-to-yarn-from-java-code.md)
===========================================


How To Run Python Programs
==========================
To run python programs just call them with `spark-submit` together with the arguments to the program.

 
Questions/Comments
==================
* [View Mahmoud Parsian's profile on LinkedIn](http://www.linkedin.com/in/mahmoudparsian)
* Please send me an email: mahmoud.parsian@yahoo.com
* [Twitter: @mahmoudparsian](http://twitter.com/mahmoudparsian) 

Thank you!
````
best regards,
Mahmoud Parsian
````

[![Data Algorithms Book](./misc/data_algorithms_image.jpg)](http://shop.oreilly.com/product/0636920033950.do)
