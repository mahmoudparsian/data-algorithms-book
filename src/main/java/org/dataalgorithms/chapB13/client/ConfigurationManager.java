package org.dataalgorithms.chapB13.client;

import java.io.File;
import java.io.IOException;
//
import org.apache.hadoop.conf.Configuration;
//
import org.apache.log4j.Logger;

/**
 * ConfigurationManager create a Hadoop Configuration object
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class ConfigurationManager {

    private static final Logger THE_LOGGER = Logger.getLogger(ConfigurationManager.class);
    //
    //
    //
    static String HADOOP_HOME = "/Users/mparsian/zmp/zs/hadoop-2.6.3";
    static String HADOOP_CONF_DIR = "/Users/mparsian/zmp/zs/hadoop-2.6.3/etc/hadoop";

    // identify the cluster: note that we may define many clusters 
    // and submit it to different clusters based on parameters and conditions   
    //
    static final String HADOOP_CONF_CORE_SITE = ConfigurationManager.getHadoopConfDir() + "/core-site.xml";
    static final String HADOOP_CONF_HDFS_SITE = ConfigurationManager.getHadoopConfDir() + "/hdfs-site.xml";
    static final String HADOOP_CONF_MAPRED_SITE = ConfigurationManager.getHadoopConfDir() + "/mapred-site.xml";
    static final String HADOOP_CONF_YARN_SITE = ConfigurationManager.getHadoopConfDir() + "/yarn-site.xml";

    //
    public static void setHadoopHomeDir(String dir) {
        HADOOP_HOME = dir;
    }

    public static String getHadoopHomeDir() {
        return HADOOP_HOME;
    }

    //
    public static void setHadoopConfDir(String dir) {
        HADOOP_CONF_DIR = dir;
    }

    public static String getHadoopConfDir() {
        return HADOOP_CONF_DIR;
    }

    static Configuration createConfiguration() throws IOException {
        //
        THE_LOGGER.info("createConfiguration() started.");
        THE_LOGGER.info("createConfiguration() HADOOP_HOME=" + ConfigurationManager.getHadoopHomeDir());
        THE_LOGGER.info("createConfiguration() HADOOP_CONF_DIR=" + ConfigurationManager.getHadoopConfDir());
        //
        Configuration config = new Configuration();
        //
        config.addResource(new File(HADOOP_CONF_CORE_SITE).getAbsoluteFile().toURI().toURL());   // WORKED
        config.addResource(new File(HADOOP_CONF_HDFS_SITE).getAbsoluteFile().toURI().toURL());   // WORKED
        config.addResource(new File(HADOOP_CONF_MAPRED_SITE).getAbsoluteFile().toURI().toURL()); // WORKED
        config.addResource(new File(HADOOP_CONF_YARN_SITE).getAbsoluteFile().toURI().toURL());   // WORKED
        //
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // WORKED
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());         // WORKED 
        config.set("hadoop.home.dir", ConfigurationManager.getHadoopHomeDir());
        config.set("hadoop.conf.dir", ConfigurationManager.getHadoopConfDir());
        config.set("yarn.conf.dir", ConfigurationManager.getHadoopConfDir());
        //
        //config.reloadConfiguration();
        //
        THE_LOGGER.info("createConfiguration(): Configuration created.");
        return config;
    }

}
