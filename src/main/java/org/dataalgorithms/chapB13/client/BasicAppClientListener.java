package org.dataalgorithms.chapB13.client;

import scala.Option;
import org.apache.log4j.Logger;
import org.apache.spark.deploy.client.StandaloneAppClientListener;
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
    @Override
    public void executorRemoved(String id, String message, Option exitStatus, boolean p4) {
    }
    
}