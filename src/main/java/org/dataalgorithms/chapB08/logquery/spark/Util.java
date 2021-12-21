package org.dataalgorithms.chapB08.logquery.spark;

import scala.Tuple3;

/**
 * Basic Utility class
 *  
 * Usage: SparkLogQuery [logFile]
 *
 * @author Mahmoud Parsian
 */
public class Util {

  // tokens = [<ip-address><user-id><number-of-bytes><query>]
  // tokens[0] = <ip-address>
  // tokens[1] = <user-id>
  // tokens[2] = <number-of-bytes>
  // tokens[3] = <query>
  public static Tuple3<String, String, String> createKey(String[] tokens) {
     String userID = tokens[1];
     if (userID.equals("-")) {
        // undefined user
        return new Tuple3<String, String, String>(null, null, null);
     }
     else {
       String ipAddress = tokens[0];
       // String userID = tokens[1];
       String query = tokens[3];
       return new Tuple3<String, String, String>(ipAddress, userID, query);
     }
  }

  // tokens = [<ip-address><user-id><number-of-bytes><query>]
  // tokens[0] = <ip-address>
  // tokens[1] = <user-id>
  // tokens[2] = <number-of-bytes>
  // tokens[3] = <query>
  public static LogStatistics createLogStatistics(String[] tokens) {
     String numberOfBytesAsString = tokens[2];
     if (numberOfBytesAsString.equals("-")) {
        return new LogStatistics(1, 0);
     }
     else {
        int numberOfBytes = Integer.parseInt(numberOfBytesAsString);
        return new LogStatistics(1, numberOfBytes);
     } 
  }
  
}
