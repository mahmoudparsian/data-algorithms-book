package org.dataalgorithms.chapB13.client;

import org.apache.log4j.Logger;
//
import org.apache.spark.launcher.SparkLauncher;


/**
 * This class submits a Spark Pi to a Spark Cluster
 * from a Java client (as opposed to submitting a Spark 
 * job from a shell command line using spark-submit).
 * 
 * To accomplish submitting a Spark job from a Java  
 * client, we use the SparkLauncher class.
 *
 *  @since Spark-2.1.0

 *  @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class SubmitSparkPiToClusterFromJavaCode {

    static final Logger THE_LOGGER = Logger.getLogger(SubmitSparkPiToClusterFromJavaCode.class);

    public static void main(String[] arguments) throws Exception {
        long startTime = System.currentTimeMillis();
        submit(arguments); // ... the code being measured ...    
        long estimatedTime = System.currentTimeMillis() - startTime;
        THE_LOGGER.info("estimatedTime (millis)=" + estimatedTime);
    }

    static void submit(String[] arguments) throws Exception {
        //
        final String javaHome = "/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home";
        final String sparkHome = "/Users/mparsian/spark-2.1.0";
        final String appResource = "/Users/mparsian/spark-2.1.0/examples/jars/spark-examples_2.11-2.1.0.jar";
        final String mainClass = "org.apache.spark.examples.SparkPi";
        //
        // parameters passed to the  SparkPi
        final String[] appArgs = new String[]{
            //"--arg",
            "5"
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
