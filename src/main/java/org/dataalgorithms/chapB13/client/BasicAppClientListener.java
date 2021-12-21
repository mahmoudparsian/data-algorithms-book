package org.dataalgorithms.chapB13.client;

import scala.Option;
import org.apache.log4j.Logger;
import org.apache.spark.deploy.client.StandaloneAppClientListener;
import org.apache.spark.scheduler.ExecutorDecommissionInfo;

/**
 * This is a very basic AppClientListener.
 * 
 *  @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 * 
 */
public class BasicAppClientListener implements StandaloneAppClientListener  {
    
     static final Logger THE_LOGGER = Logger.getLogger(BasicAppClientListener.class);
   
    @Override
    public void connected(String id) {
      THE_LOGGER.info("Connected to master, got app ID " + id);
    }

    @Override
    public void disconnected() {
      THE_LOGGER.info("Disconnected from master");
      System.exit(0);
    }

    @Override
    public void dead(String reason) {
      THE_LOGGER.info("Application died with error: " + reason);
      System.exit(0);
    }

    @Override
    public void  executorAdded(String id, String workerId, String hostPort, int cores, int memory) {
    }

    //scala: Option[Int] exitStatus
    // p4 needs a description
    public void executorRemoved(String id, String message, Option exitStatus, boolean p4) {
    }
    
     @Override
    public void executorRemoved(String fullId, String message, Option<Object> exitStatus, Option<String> workerHost) {
    }
     
    /**
     *
     * @param str1
     * @param str2
     * @param str3
     */
    @Override
    public void workerRemoved(String str1, String str2, String str3) {
    } 
    
    @Override
    public void executorDecommissioned(String fullId, ExecutorDecommissionInfo decommissionInfo){
        
    }

}