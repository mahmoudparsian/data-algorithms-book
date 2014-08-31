Getting JARs for the "lib" Directory
====================================
GitHub does not let to push [large files](https://help.github.com/articles/what-is-my-disk-quota) (size of bigger than 100MB) to GIT repository. I have one jar, which is larger than 100 MB, therefore, you should copy that jar to you "lib" directory.  There are at least three options (Option-1, Option-2, and Option-3) to get that jar file:

Option-1: Let Ant copy it for you 
=================================
The first time you build by

        ant
    
The `spark-assembly-1.0.2-hadoop2.5.0.jar` will be copied/downloaded from the following URL directory:

        http://www.mapreduce4hackers.com/dataalgorithmsbook/lib/
    
Subsequent builds will not copy the big jar file.

Sample [log](./misc/option-1-log.txt) of Option-1 is provided.

Option-2: Download 
==================
You may download that jar file from the following URL and then copy it to the `lib` directory:

* [spark-assembly-1.0.2-hadoop2.5.0.jar](http://www.mapreduce4hackers.com/dataalgorithmsbook/lib/spark-assembly-1.0.2-hadoop2.5.0.jar)
* Size of this jar is: 130,007,545 bytes
* This jar is built using Spark 1.0.2 against Hadoop 2.5.0
    

Option-3: Build Your Own JAR from Spark Source
==============================================
You may build this jar file from the Spark 1.0.2 version: this is how:
* Download [Spark 1.0.2](http://d3kbcqa49mib13.cloudfront.net/spark-1.0.2.tgz) to your designated directory:

            <your-install-dir>/spark-1.0.2.tgz
   
* Open it it up by

            cd <your-install-dir>
            tar zvfx spark-1.0.2.tgz
            cd spark-1.0.2
            SPARK_HADOOP_VERSION=2.5.0 SPARK_YARN=true sbt/sbt assembly


* Once your build is successful, then you will have your desired jar file as:
    
            <your-install-dir>/spark-1.0.2/assembly/target/scala-2.10/spark-assembly-1.0.2-hadoop2.5.0.jar

* Finally, copy `spark-assembly-1.0.2-hadoop2.5.0.jar` to the `lib` directory.



Thanks,

best regards,

Mahmoud Parsian
