package org.dataalgorithms.chapB08.logquery.spark;

import java.io.Serializable;

/** 
 * Keep Track of the total query count and number of aggregate 
 * bytes for a certain group of users
 *
 * @author Mahmoud Parsian
 *
 */
public  class LogStatistics implements Serializable {

   private final int count;
   private final int numberOfBytes;

   public LogStatistics(int count, int numberOfBytes) {
      this.count = count;
      this.numberOfBytes = numberOfBytes;
   }
    
   public LogStatistics merge(LogStatistics other) {
      return new LogStatistics(this.count + other.count, 
                               this.numberOfBytes + other.numberOfBytes);
   }

   @Override
   public String toString() {
      return String.format("count=%s\t numberOfbytes=%s\n", this.count, this.numberOfBytes);
   }
}