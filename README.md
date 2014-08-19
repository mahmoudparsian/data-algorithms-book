Data Algorithms Book
====================
![Data Algorithms Book](http://akamaicovers.oreilly.com/images/0636920033950/rc_lrg.jpg)


This repository will host all source code and scripts for "Data Algorithms" Book.
This book provides a set of MapReduce algrithms, which are implemented using Hadoop 2.4.1 and Spark 1.0.0.

URL To Data Algorithms Book
===========================
* Title: [Data Algorithms](http://shop.oreilly.com/product/0636920033950.do)
* Author: Mahmoud Parsian
* Publisher: O'Reilly Media 

Source Code
===========
All source code and scripts will be posted here soon...

Software Used
=============
* JDK6/JDK7
* Hadoop 2.4.1
* Spark 1.0.0
 
Structure of Repository
=======================

Name         | Description
------------ |------------
README.md    | The file you are reading now
src          | Source files for MapReduce/Hadoop/Spark
lib          | Required jar files
build.xml    | The ant build script
dist         | The ant build's output directory 
LICENSE      | License for using this repository
misc         | misc. files for this repository

Structure of src Directory
==========================
![src directory](https://raw.githubusercontent.com/mahmoudparsian/DataAlgorithmsBook/master/misc/source_tree.png)


How To Build
============
[Apache's ant 1.9.4](http://ant.apache.org/) is used for building the project.

* To clean up:

  **ant clean**

* To build: the build will create <install-dir>/dist/data_algorithms_book.jar file.

  **ant**

How To Run Programs
===================
 To run programs, you have to make sure that your CLASSPATH contains  
 the  `<install-dir>/dist/data_algorithms_book.jar` and all jar files  
 in the `<install-dir>/lib/` directory. Make sure that you use the full  
 path for all jar files.
