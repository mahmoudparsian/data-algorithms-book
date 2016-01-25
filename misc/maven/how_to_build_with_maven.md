How To Build with Apache's Maven
================================

Installing Maven
----------------
To build with Maven, you should install [Apache's Maven](https://maven.apache.org).
Here are the steps for installing Maven:

* [Installing Apache Maven](https://maven.apache.org/install.html)

* [Maven Install on Mac OS X](http://stackoverflow.com/questions/8826881/maven-install-on-mac-os-x)


Check Installation of Maven
---------------------------
To test that if you have successfully installed Maven, 
you may do the following checks:

````
# mvn -version
Apache Maven 3.3.9 (bb52d8502b132ec0a5a3f4c09453c07478323dc5; 2015-11-10T08:41:47-08:00)
Maven home: /usr/local/Cellar/maven/3.3.9/libexec
Java version: 1.7.0_60, vendor: Oracle Corporation
Java home: /Library/Java/JavaVirtualMachines/jdk1.7.0_60.jdk/Contents/Home/jre
Default locale: en_US, platform encoding: UTF-8
OS name: "mac os x", version: "10.11.1", arch: "x86_64", family: "mac"

# type mvn
mvn is hashed (/usr/local/bin/mvn)
````

Building with Apache's Maven
----------------------------
* To clean up:

  **mvn clean**

* To build:

  **mvn package**


The successful build will create ````<install-dir-data-algorithms-book>/target/data-algorithms-1.0.0.jar```` 
file.


Sample Maven Build Log
----------------------
````
# type mvn
mvn is hashed (/usr/local/bin/mvn)

# mvn clean
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building data-algorithms 1.0.0
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-clean-plugin:2.5:clean (default-clean) @ data-algorithms ---
[INFO] Deleting /home/mparsian/github/data-algorithms-book/target
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 0.289 s
[INFO] Finished at: 2016-01-24T10:37:45-08:00
[INFO] Final Memory: 6M/245M
[INFO] ------------------------------------------------------------------------

# mvn package
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building data-algorithms 1.0.0
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ data-algorithms ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /home/mparsian/github/data-algorithms-book/src/main/resources
[INFO]
[INFO] --- maven-compiler-plugin:3.1:compile (default-compile) @ data-algorithms ---
[INFO] Changes detected - recompiling the module!
[WARNING] File encoding has not been set, using platform encoding UTF-8, i.e. build is platform dependent!
[INFO] Compiling 303 source files to /home/mparsian/github/data-algorithms-book/target/classes
[WARNING] /home/mparsian/github/data-algorithms-book/src/main/java/org/dataalgorithms/parquet/TestUtils.java: Some input files use or override a deprecated API.
[WARNING] /home/mparsian/github/data-algorithms-book/src/main/java/org/dataalgorithms/parquet/TestUtils.java: Recompile with -Xlint:deprecation for details.
[WARNING] /home/mparsian/github/data-algorithms-book/src/main/java/org/dataalgorithms/bonus/rankproduct/spark/SparkRankProductUsingCombineByKey.java: Some input files use unchecked or unsafe operations.
[WARNING] /home/mparsian/github/data-algorithms-book/src/main/java/org/dataalgorithms/bonus/rankproduct/spark/SparkRankProductUsingCombineByKey.java: Recompile with -Xlint:unchecked for details.
[INFO]
[INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ data-algorithms ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /home/mparsian/github/data-algorithms-book/src/test/resources
[INFO]
[INFO] --- maven-compiler-plugin:3.1:testCompile (default-testCompile) @ data-algorithms ---
[INFO] Changes detected - recompiling the module!
[WARNING] File encoding has not been set, using platform encoding UTF-8, i.e. build is platform dependent!
[INFO] Compiling 2 source files to /home/mparsian/github/data-algorithms-book/target/test-classes
[INFO]
[INFO] --- maven-surefire-plugin:2.12.4:test (default-test) @ data-algorithms ---
[INFO] Surefire report directory: /home/mparsian/github/data-algorithms-book/target/surefire-reports

-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running org.dataalgorithms.chap05.mapreduce.PairOfWordsTest
pair=(w1, w2) 1
pair=(w1, w3) 1
pair=(w1, *)  2
pair=(w2, w1) 1
pair=(w2, w3) 1
pair=(w2, w4) 1
pair=(w2, *)  3
pair=(w3, w1) 1
pair=(w3, w2) 1
pair=(w3, w4) 1
pair=(w3, w5) 1
pair=(w3, *)  4
pair=(w4, w2) 1
pair=(w4, w3) 1
pair=(w4, w5) 1
pair=(w4, w6) 1
pair=(w4, *)  4
pair=(w5, w3) 1
pair=(w5, w4) 1
pair=(w5, w6) 1
pair=(w5, *)  3
pair=(w6, w4) 1
pair=(w6, w5) 1
pair=(w6, *)  2
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.153 sec

Results :

Tests run: 1, Failures: 0, Errors: 0, Skipped: 0

[INFO]
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ data-algorithms ---
[INFO] Building jar: /home/mparsian/github/data-algorithms-book/target/data-algorithms-1.0.0.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 6.163 s
[INFO] Finished at: 2016-01-24T10:37:58-08:00
[INFO] Final Memory: 53M/462M
[INFO] ------------------------------------------------------------------------


# ls -l target
total 1232
drwxr-xr-x  ...     102 Jan 24 10:44 classes
-rw-r--r--  ...  627936 Jan 24 10:44 data-algorithms-1.0.0.jar
drwxr-xr-x  ...     102 Jan 24 10:44 maven-archiver
drwxr-xr-x  ...     102 Jan 24 10:44 maven-status
drwxr-xr-x  ...     136 Jan 24 10:44 surefire-reports
drwxr-xr-x  ...     102 Jan 24 10:44 test-classes
````