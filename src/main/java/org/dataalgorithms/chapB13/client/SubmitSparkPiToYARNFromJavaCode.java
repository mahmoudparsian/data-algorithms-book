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
 Usage: org.apache.spark.deploy.yarn.Client [options]
 Options:
  --jar JAR_PATH           Path to your application's JAR file (required in yarn-cluster mode)
  --class CLASS_NAME       Name of your application's main class (required)
  --primary-py-file        A main Python file
  --arg ARG                Argument to be passed to your application's main class. 
                           Multiple invocations are possible, each will be passed in order.
  --num-executors NUM      Number of executors to start (Default: 2)
  --executor-cores NUM     Number of cores per executor (Default: 1).
  --driver-memory MEM      Memory for driver (e.g. 1000M, 2G) (Default: 512 Mb)
  --driver-cores NUM       Number of cores used by the driver (Default: 1).
  --executor-memory MEM    Memory per executor (e.g. 1000M, 2G) (Default: 1G)
  --name NAME              The name of your application (Default: Spark)
  --queue QUEUE            The hadoop queue to use for allocation requests (Default: 'default')
  --addJars jars           Comma separated list of local jars that want SparkContext.addJar to work with.
  --py-files PY_FILES      Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.
  --files files            Comma separated list of files to be distributed with the job.
  --archives archives      Comma separated list of archives to be distributed with the job.
  
  
  How to call this program example:
  
     export SPARK_HOME="/Users/mparsian/spark-1.6.1-bin-hadoop2.6"
     java -DSPARK_HOME="$SPARK_HOME" org.dataalgorithms.chapB13.client.SubmitSparkPiToYARNFromJavaCode 10

* 
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
        String[] args = new String[]{
            "--name",
            "test-SparkPi",
            //
            "--driver-memory",
            "1000M",
            //
            "--jar",
            //SPARK_HOME + "/examples/target/scala-2.10/spark-examples-1.6.0-hadoop2.5.0.jar",
            SPARK_HOME + "/lib/spark-examples-1.6.1-hadoop2.6.0.jar",            
            //
            "--class",
            "org.apache.spark.examples.SparkPi",
            // argument 1 to my Spark program
            "--arg",
            slices,
            // argument 2 to my Spark program (helper argument to create a proper JavaSparkContext object)
            "--arg",
            "yarn-cluster"
        };

        Configuration config = new Configuration();
        //
        System.setProperty("SPARK_YARN_MODE", "true");
        //
        SparkConf sparkConf = new SparkConf();
        ClientArguments clientArgs = new ClientArguments(args, sparkConf); // worked
        Client client = new Client(clientArgs, config, sparkConf);
        //
        client.run();
        // done!
    }
}
