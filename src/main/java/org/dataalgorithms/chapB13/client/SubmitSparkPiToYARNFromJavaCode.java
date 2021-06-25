package org.dataalgorithms.chapB13.client;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
//
import org.apache.hadoop.conf.Configuration;
//
import org.apache.log4j.Logger;

/**
 * This class submits a SparkPi to a YARN from a Java client (as opposed 
 * to submitting a Spark job from a shell command line using spark-submit).
 * 
 * To accomplish submitting a Spark job from a Java client, we use 
 * the org.apache.spark.deploy.yarn.Client class described below:
 * 


|Usage: org.apache.spark.deploy.yarn.Client [options]
      |Options:
      |  --jar JAR_PATH           Path to your application's JAR file (required in yarn-cluster mode)
      |  --class CLASS_NAME       Name of your application's main class (required)
      |  --primary-py-file        A main Python file
      |  --primary-r-file         A main R file
      |  --arg ARG                Argument to be passed to your application's main class.
      |                           Multiple invocations are possible, each will be passed in order.
  
  How to call this program example:
  
     export SPARK_HOME="/Users/mparsian/spark-2.1.0"
     java -DSPARK_HOME="$SPARK_HOME" org.dataalgorithms.chapB13.client.SubmitSparkPiToYARNFromJavaCode 5

*  @since Spark-2.1.0

*  @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
* 
*/
public class SubmitSparkPiToYARNFromJavaCode {

    static final Logger THE_LOGGER = Logger.getLogger(SubmitSparkPiToYARNFromJavaCode.class);

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        //
        String slices = args[0];  // this is passed to SparkPi program 
        //
        String SPARK_HOME = System.getProperty("SPARK_HOME");
        THE_LOGGER.info("SPARK_HOME=" + SPARK_HOME);
        //
        pi(SPARK_HOME, slices); // ... the code being measured ... 
        //
        long elapsedTime = System.currentTimeMillis() - startTime;
        THE_LOGGER.info("elapsedTime (millis)=" + elapsedTime);
    }

    static void pi(String SPARK_HOME, String slices) throws Exception {
        // 
        String sparkExamplesJar = "file://" + SPARK_HOME + "/examples/jars/spark-examples_2.11-2.0.0.jar";
        //String sparkExamplesJar = "/spark/spark-examples_2.11-2.0.0.jar"; // HDFS
        THE_LOGGER.info("sparkExamplesJar=" + sparkExamplesJar);
        //
        final String[] args = new String[]{
            //"--name",
            //"test-SparkPi",
            
            //
            //"--driver-memory",
            //"1000M",
            
            //
            "--jar",
            sparkExamplesJar,
            
            //
            "--class",
            "org.apache.spark.examples.SparkPi",
            
            // argument 1 to my Spark program
            "--arg",
            slices
            
            // argument 2 to my Spark program (helper argument to create a proper JavaSparkContext object)
            //"--arg",
            //"yarn-cluster"
            //"yarn"
        };

        //Configuration config = new Configuration();
        Configuration config = ConfigurationManager.createConfiguration();     
        //
        System.setProperty("SPARK_YARN_MODE", "true");
        //
        SparkConf sparkConf = new SparkConf();
        sparkConf.setSparkHome(SPARK_HOME);
        
        sparkConf.setMaster("yarn");
        //sparkConf.setMaster("yarn-cluster");
        
        sparkConf.setAppName("spark-yarn");
        sparkConf.set("master", "yarn");
        
        sparkConf.set("spark.submit.deployMode", "cluster"); // worked
        //sparkConf.set("spark.submit.deployMode", "client");  // did not work
        
        //sparkConf.set("spark.yarn.am.port", "8088"); 
        //sparkConf.set("spark.driver.host", "localhost");
        //sparkConf.set("spark.driver.host", "mahmoudsmacbook.illumina.com");
        //sparkConf.set("spark.driver.port", "8088");  
        //sparkConf.set("spark.yarn.access.namenodes", "hdfs://localhost:9000");
        
        //
        ClientArguments clientArguments = new ClientArguments(args);                 // spark-2.0.0
        //Client client = new Client(clientArguments, config, sparkConf);
        Client client = new Client(clientArguments, sparkConf, null);
        //
        client.run();
        // done!
    }
}
