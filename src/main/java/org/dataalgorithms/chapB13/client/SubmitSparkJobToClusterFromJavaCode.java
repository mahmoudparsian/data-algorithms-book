package org.dataalgorithms.chapB13.client;

import org.apache.log4j.Logger;
//
import org.apache.spark.launcher.SparkLauncher;


/**
 * This class submits a Spark job to a Spark Cluster
 * from a Java client (as opposed to submitting a Spark 
 * job from a shell command line using spark-submit).
 * 
 * To accomplish submitting a Spark job from a Java  
 * client, we use the SparkLauncher class.
 *
 * 
 *  @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class SubmitSparkJobToClusterFromJavaCode {

    static final Logger THE_LOGGER = Logger.getLogger(SubmitSparkJobToClusterFromJavaCode.class);

    public static void main(String[] arguments) throws Exception {
        long startTime = System.currentTimeMillis();
        test(arguments); // ... the code being measured ...    
        long estimatedTime = System.currentTimeMillis() - startTime;
        THE_LOGGER.info("estimatedTime (millis)=" + estimatedTime);
    }

    static void test(String[] arguments) throws Exception {
        //
        final String javaHome = "/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home";
        final String sparkHome = "/Users/mparsian/spark-2.1.0";
        final String appResource = "/Users/mparsian/zmp/github/data-algorithms-book/dist/data_algorithms_book.jar";
        final String mainClass = "org.dataalgorithms.bonus.friendrecommendation.spark.SparkFriendRecommendation";
        //
        // parameters passed to the  SparkFriendRecommendation
        final String[] appArgs = new String[]{
            //"--arg",
            "3",
            
            //"--arg",
            "/friends/input",
            
            //"--arg",
            "/friends/output"
        };
        //
        //
        SparkLauncher spark = new SparkLauncher()
                .setVerbose(true)
                .setJavaHome(javaHome)
                .setSparkHome(sparkHome)
                .setAppResource(appResource)    // "/my/app.jar"
                .setMainClass(mainClass)        // "my.spark.app.Main"
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .addAppArgs(appArgs);
        //
        // Launches a sub-process that will start the configured Spark application.
        Process proc = spark.launch();
        //
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(proc.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();
        //
        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(proc.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();
        //
        THE_LOGGER.info("Waiting for finish...");
        int exitCode = proc.waitFor();
        THE_LOGGER.info("Finished! Exit code:" + exitCode);
    }
}
