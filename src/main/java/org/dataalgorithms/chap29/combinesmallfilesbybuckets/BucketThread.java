package org.dataalgorithms.chap29.combinesmallfilesbybuckets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import org.dataalgorithms.util.InputOutputUtil;

/**
 * The class enable us to concatenate small files into one big file,
 * which will be less than the block size.
 * This way, we will submit less map() jobs with big input.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class BucketThread implements Runnable {

 	private static Logger theLogger = Logger.getLogger(BucketThread.class);
    private static final Path NULL_PATH = new Path("/tmp/null");
    
    private Thread runner = null;
    private List<Path> bucket = null;
    private Configuration conf = null;
    private FileSystem fs = null;
    private int id;                        // each bucket has a unique id 
    private String parentDir = null;       // "/tmp/<uuid>/" (trailing / will be part of parentDir)
    private String targetDir = null;
    private String targetFile = null;
    
    /**
     *  Create BucketThread object.
     *
     *  parentDir will  be "/tmp/<uuid>/"
     *  targetDir will  be "/tmp/<uuid>/id/"
     *  targetFile will be "/tmp/<uuid>/id/id"
     *
     */     
     public BucketThread(String parentDir, int id, Configuration conf) throws IOException {
        this.id = id;
        this.parentDir = parentDir;
        this.targetDir = parentDir + id;
        this.targetFile = targetDir + "/" + id;
        this.conf = conf;
        this.runner = new Thread(this); 
        this.fs = FileSystem.get(this.conf);
        this.bucket = new ArrayList<Path>();
    }
    
    public void start() {
        // Start the thread.
        runner.start();
    }    
    
    public void join() throws InterruptedException {
        // join and wait for other threads
        runner.join();
    }    
    
    public void run() {
        // execute the core of thread
        try {
            copyMerge();
        }
        catch(Exception e) {
            theLogger.error("run(): copyMerge() failed.", e);
        }
    }
        
    /**
     * @param path is a directory
     */
    public void add(String path) {
        if (path == null) {
            return;
        }
        
        Path hdfsPath = new Path(path);
        if (pathExists(hdfsPath)) {    
            bucket.add(hdfsPath);
        }
    }
    
    public List<Path> getBucket() {
        return bucket;
    }
    
    public int size() {
        return bucket.size();
    }    
    
    public Path getTargetDir() {
        if ( size() == 0 ) {
            // an empty directory, which has no files it
            return NULL_PATH; 
        }
        else if (size() == 1 ) {
            return bucket.get(0);
        }
        else {
            // bucket has 2 or more items (which we have merged)
            return new Path(targetDir);
        }
    }
    
    /** 
     * Copy all files in several directories to one output file (merge). 
     * 
     *  parentDir will  be "/tmp/<uuid>/"
     *  targetDir will  be "/tmp/<uuid>/id/"
     *  targetFile will be "/tmp/<uuid>/id/id"
     *
     *  merge all paths in bucket and return a new directory (targetDir), which holds merged paths
     */
    public void copyMerge() throws IOException {
        
        // if there is only one path/dir in the bucket, then there is no need to merge it
        if ( size() < 2 ) {
            return;
        }
        
        // here size() >= 2
        Path hdfsTargetFile = new Path(targetFile);                           
        OutputStream out = fs.create(hdfsTargetFile);    
        try {
            for (int i = 0; i < bucket.size(); i++) {
                FileStatus contents[] = fs.listStatus(bucket.get(i));
                for (int k = 0; k < contents.length; k++) {
                    if (!contents[k].isDir()) {
                        InputStream in = fs.open(contents[k].getPath());
                        try {
                            IOUtils.copyBytes(in, out, conf, false);                
                        } 
                        finally {
                            InputOutputUtil.close(in);
                        } 
                    }
                } //for k
                
            } // for i 
        } 
        finally {
            InputOutputUtil.close(out);
        }
        
    } 
                 
    /**
     * Return true, if HDFS path doers exist; otherwise return false.
     * 
     */
    public boolean pathExists(Path path)  {
        if (path == null) {
            return false;
        }
        
        try {
            return fs.exists(path);
        }
        catch(Exception e) {
            //theLogger.error("pathExists(): failed.", e);
            return false;
        }
    }
        
    public String toString() {
        return bucket.toString();
    }

}
