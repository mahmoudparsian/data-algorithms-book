Submit a Spark job to YARN from Java Code
=========================================
In this post I will show you how to submit a Spark job from Java code. Typically, we submit 
Spark jobs to "Spark Cluster" and Hadoop/YARN  by using ````$SPARK_HOME/bin/spark-submit```` 
shell script.  Submitting Spark job from a shell script limits programmers when they want to submit 
Spark jobs from Java code (such as Java servlets or other Java code such as REST servers).

Use YARN's Client Class
=======================
Below is a complete Java code, which submits a Spark job to YARN from Java code (no shell scripting 
is required). The main class used for submitting a Spark job to YARN is the ```org.apache.spark.deploy.yarn.Client```` class.


Complete Java Client Program
============================

````
$ cat SubmitSparkJobToYARNFromJavaCode.java

// import required classes and interfaces
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

public class SubmitSparkJobToYARNFromJavaCode {

   public static void main(String[] arguments) throws Exception {

       // prepare arguments to be passed to 
       // org.apache.spark.deploy.yarn.Client object
       String[] args = new String[] {
           // the name of your application
           "--name",
           "myname",
           
           // memory for driver (optional)
           "--driver-memory",
           "1000M",
              
           // path to your application's JAR file 
           // required in yarn-cluster mode      
           "--jar",
           "/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar",

           // name of your application's main class (required)
           "--class",
           "org.dataalgorithms.bonus.friendrecommendation.spark.SparkFriendRecommendation",

           // comma separated list of local jars that want 
           // SparkContext.addJar to work with		
           "--addJars",
           "/Users/mparsian/zmp/github/data-algorithms-book/lib/spark-assembly-1.5.2-hadoop2.6.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/log4j-1.2.17.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/junit-4.12-beta-2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jsch-0.1.42.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/JeraAntTasks.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jedis-2.5.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/jblas-1.2.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/hamcrest-all-1.3.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/guava-18.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math3-3.0.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-math-2.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-logging-1.1.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang3-3.4.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-lang-2.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-io-2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-httpclient-3.0.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-daemon-1.0.5.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-configuration-1.6.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-collections-3.2.1.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/commons-cli-1.2.jar,/Users/mparsian/zmp/github/data-algorithms-book/lib/cloud9-1.3.2.jar",

           // argument 1 to your Spark program (SparkFriendRecommendation)
           "--arg",
           "3",

           // argument 2 to your Spark program (SparkFriendRecommendation)
           "--arg",
           "/friends/input",

           // argument 3 to your Spark program (SparkFriendRecommendation)
           "--arg",
           "/friends/output",
  
           // argument 4 to your Spark program (SparkFriendRecommendation)
           // this is a helper argument to create a proper JavaSparkContext object
           // make sure that you create the following in SparkFriendRecommendation program
           // ctx = new JavaSparkContext("yarn-cluster", "SparkFriendRecommendation");
           "--arg",
           "yarn-cluster"
       };
       
       // create a Hadoop Configuration object
       Configuration config = new Configuration();

       // identify that you will be using Spark as YARN mode
       System.setProperty("SPARK_YARN_MODE", "true");

       // create an instance of SparkConf object
       SparkConf sparkConf = new SparkConf();

       // create ClientArguments, which will be passed to Client
       ClientArguments cArgs = new ClientArguments(args, sparkConf); 
       
       // create an instance of yarn Client client
       Client client = new Client(cArgs, config, sparkConf); 
                
       // submit Spark job to YARN
       client.run(); 
   }
}
````

Running Complete Java Client Program
====================================

Now, we can submit Spark job from Java code:
````
$ javac  SubmitSparkJobToYARNFromJavaCode.java
$ java  SubmitSparkJobToYARNFromJavaCode
````

Note: before running your Java code, make sure that the 
output directory does not exist:
````
hadoop fs -rmr /friends/output
````
