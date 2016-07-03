package org.dataalgorithms.util;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.IOException;
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;



/**
 * This class provides convenient methods for accessing  
 * Hadoop distributed file system (HDFS).
 *
 * @author Mahmoud Parsian
 *
 */
public class HadoopUtil {

   /**
    * Add all jar files to HDFS's distributed cache
    *
    * @param job job which will be run
    * @param hdfsJarDirectory a directory which has all required jar files
    */ 
   public static void addJarsToDistributedCache(Job job, 
                                                String hdfsJarDirectory) 
      throws IOException {
      if (job == null) {
         return;
      }
      addJarsToDistributedCache(job.getConfiguration(), hdfsJarDirectory);
   }

   /**
    * Add all jar files to HDFS's distributed cache
    *
    * @param Configuration conf which will be run
    * @param hdfsJarDirectory a directory which has all required jar files
    */ 
   public static void addJarsToDistributedCache(Configuration conf, 
                                                String hdfsJarDirectory) 
      throws IOException {
      if (conf == null) {
         return;
      }
      FileSystem fs = FileSystem.get(conf);
      List<FileStatus> jars = getDirectoryListing(hdfsJarDirectory, fs);
      for (FileStatus jar : jars) {
         Path jarPath = jar.getPath();
         DistributedCache.addFileToClassPath(jarPath, conf, fs);
      }
   }

   
   /**
    * Get list of files from a given HDFS directory
    * @param directory an HDFS directory name
    * @param fs an HDFS FileSystem
    */   
    public static List<FileStatus> getDirectoryListing(String directory, 
                                                       FileSystem fs) 
       throws IOException {
       Path dir = new Path(directory); 
       FileStatus[] fstatus = fs.listStatus(dir); 
       return Arrays.asList(fstatus);
    }
    
    public static List<String> listDirectoryAsListOfString(String directory, 
                                                           FileSystem fs) 
       throws IOException {
       Path path = new Path(directory); 
       FileStatus fstatus[] = fs.listStatus(path);
       List<String> listing = new ArrayList<String>();
       for (FileStatus f: fstatus) {
           listing.add(f.getPath().toUri().getPath());
       }
       return listing;
    }
    
    
   /**
    * Return true, if HDFS path doers exist; otherwise return false.
    * 
    */
   public static boolean pathExists(Path path, FileSystem fs)  {
      if (path == null) {
         return false;
      }
      
      try {
         return fs.exists(path);
      }
      catch(Exception e) {
          return false;
      }
   }   
   
}
