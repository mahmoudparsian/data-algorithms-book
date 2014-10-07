package org.dataalgorithms.chap29.combinesmallfilesbybuckets;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * SmallFilesConsolidator is used to provide common functionality to Hadoop Job Drivers.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class SmallFilesConsolidator {

	private static Logger THE_LOGGER = Logger.getLogger(SmallFilesConsolidator.class);
    
    // this directory is configurable
    private static String MERGED_HDFS_ROOT_DIR = "/tmp/";

    public static int getNumberOfBuckets(int totalFiles, 
                                         int numberOfMapSlotsAvailable,
                                         int maxFilesPerBucket) {
        if (totalFiles <= (maxFilesPerBucket * numberOfMapSlotsAvailable)) {
            return numberOfMapSlotsAvailable;
        }
        else {
            int numberOfBuckets = totalFiles / maxFilesPerBucket;
            int remainder = totalFiles % maxFilesPerBucket; 
            if (remainder == 0) {
                // then we have exact number of buckets
                return numberOfBuckets; 
            }
            else {
                return numberOfBuckets+1;
            }
        }    
    }
    
    
    /**
     * Create required buckets for mappers
     *
     */
    public static BucketThread[]  createBuckets(int totalFiles,
                                                int numberOfMapSlotsAvailable,
                                                int maxFilesPerBucket) {                        
        int numberOfBuckets = getNumberOfBuckets(totalFiles, numberOfMapSlotsAvailable, maxFilesPerBucket);
        BucketThread[] buckets = new BucketThread[numberOfBuckets];    
        return buckets;
    }        


    /**
     * fill buckets... 
     *
     */
    public static void fillBuckets(
        BucketThread[] buckets, 
        List<String> smallFiles, // list of small files
        Job job,
        int maxFilesPerBucket)  
        throws Exception {
        
        int numberOfBuckets = buckets.length;
        // now partition all files (defined in smallFiles) into buckets and fill them
        int combinedSize = smallFiles.size();
        int biosetsPerBucket =     combinedSize / numberOfBuckets;
        if (biosetsPerBucket < maxFilesPerBucket) {
            int remainder = combinedSize % numberOfBuckets;
            if (remainder != 0) {
                biosetsPerBucket++;
            }
        }
        
        String parentDir = getParentDir();
        int id = 0; // how many buckets we have used so far (range is from 0 to numberOfBuckets-1)
        int index = 0; // how many files used so far (range is from 1 to combinedSize)
        boolean done = false;
        while ( (!done) & (id < numberOfBuckets) ) {
            // create a single bucket
            buckets[id] = new BucketThread(parentDir, id, job.getConfiguration());        
            // fill a single bucket with some files
            for (int b = 0; b < biosetsPerBucket; b++) {
                buckets[id].add(smallFiles.get(index));
                index++;
                if (index == combinedSize) {
                    // then we are done
                    done = true;
                    break;
                }
            } // for-loop
            
            id++;
        } // while-loop
        
    }
    
    
    /**
     * now merge each bucket and return the merged target path    
     * this for-loop can be done by threads...
     *
     */
     public static void mergeEachBucket(BucketThread[] buckets, Job job) throws Exception {
        if (buckets == null) {
            return;
        }
    
        int numberOfBuckets = buckets.length;
        if (numberOfBuckets < 1) {
            return;
        }
        
        for (int ID = 0; ID < numberOfBuckets; ID++) {
            if (buckets[ID] != null) {
                buckets[ID].start();
            }
        }

        // join all threads...
        for (int ID = 0; ID < numberOfBuckets; ID++) {
            if (buckets[ID] != null) {
                buckets[ID].join();
            }
        }

        // all threads done here
        for (int ID = 0; ID < numberOfBuckets; ID++) {
            if (buckets[ID] != null) {
                Path biosetPath = buckets[ID].getTargetDir();
                addInputPathWithoutCheck(job, biosetPath);
            }
        }
    }    
    
    private static void addInputPathWithoutCheck(Job job, Path path) {
        try {
            FileInputFormat.addInputPath(job, path);
            THE_LOGGER.info("added path: "+ path);    
        }
        catch(Exception e) {
            // some data for biosets might not exist 
            THE_LOGGER.error("could not add path: "+ path, e);
        }
    }        
    
    private static String getParentDir() {
        String guid = UUID.randomUUID().toString();
         return MERGED_HDFS_ROOT_DIR + guid + "/";
    }
    
}


