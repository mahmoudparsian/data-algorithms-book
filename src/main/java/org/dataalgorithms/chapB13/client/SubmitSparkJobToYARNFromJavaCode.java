package org.dataalgorithms.chapB13.client;

import java.util.List;
import java.util.Arrays;
//
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
//
import org.apache.hadoop.conf.Configuration;
//
import org.apache.log4j.Logger;

/**
 * This class submits a Spark job to a YARN from a Java client (as opposed 
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

* 
*  @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
* 
*/

public class SubmitSparkJobToYARNFromJavaCode {
    
    static final Logger THE_LOGGER = Logger.getLogger(SubmitSparkJobToYARNFromJavaCode.class);
    
    /**
     *  or you may read these jars from a lib directory
     * 
     */
    static List<String> JARS = Arrays.asList(
        "JeraAntTasks.jar",
        "cloud9-1.3.2.jar",
        "commons-cli-1.2.jar",
        "commons-collections-3.2.1.jar",
        "commons-configuration-1.6.jar",
        "commons-daemon-1.0.5.jar",
        "commons-httpclient-3.0.1.jar",
        "commons-io-2.1.jar",
        "commons-lang-2.6.jar",
        "commons-lang3-3.4.jar",
        "commons-logging-1.1.1.jar",
        "commons-math-2.2.jar",
        "commons-math3-3.6.jar",
        "guava-18.0.jar",
        "hamcrest-all-1.3.jar",
        "jblas-1.2.3.jar",
        "jedis-2.5.1.jar",
        "jsch-0.1.42.jar",
        "junit-4.12-beta-2.jar",
        "log4j-1.2.17.jar",
        "spring-context-3.0.7.RELEASE.jar"
    );
    
    private static String buildJars(String prefixJarDir) {
        StringBuilder builder = new StringBuilder();
        int counter = 0;
        for (String jar : JARS) {
            builder.append(prefixJarDir);
            builder.append("/");            
            builder.append(jar);
            if (counter < JARS.size() -1) {
               builder.append(",");            
            }
            counter++;
        }
        return builder.toString();
    }
    
    public static void main(String[] arguments) throws Exception {
        long startTime = System.currentTimeMillis();    
        test(arguments); // ... the code being measured ...    
        long estimatedTime = System.currentTimeMillis() - startTime;   
        THE_LOGGER.info("estimatedTime (millis)="+estimatedTime);
    }        
    
    static void test(String[] arguments) throws Exception {           
        // build required jars (separated by ",")
        String prefixJarDir = arguments[0];
        THE_LOGGER.info("prefixJarDir="+prefixJarDir);
        //
        String jars = buildJars(prefixJarDir);
        THE_LOGGER.info("jars="+jars);
        //
        //
        String[] args = new String[]{
            "--name",
            "my-app-name",
            //
            "--driver-memory",
            "1000M",
            //
            "--jar",
            "/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar",
            //
            "--class",
            "org.dataalgorithms.bonus.friendrecommendation.spark.SparkFriendRecommendation",
            //
            "--addJars",
            jars,
            
            // argument 1 to my Spark program
            "--arg",
            "3",
            
            // argument 2 to my Spark program
            "--arg",
            "/friends/input",
            
            // argument 3 to my Spark program
            "--arg",
            "/friends/output",
            
            // argument 4 to my Spark program (helper argument to create a proper JavaSparkContext object)
            "--arg",
            "yarn-cluster"
        };

        Configuration config = new Configuration();
        //
        System.setProperty("SPARK_YARN_MODE", "true");
        //
        SparkConf sparkConf = new SparkConf();
        ClientArguments clientArgs = new ClientArguments(args, sparkConf);
        Client client = new Client(clientArgs, config, sparkConf);
        //
        client.run(); 
        // done!
    }
}