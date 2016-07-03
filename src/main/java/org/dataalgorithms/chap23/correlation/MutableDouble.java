package org.dataalgorithms.chap23.correlation;

import java.io.Serializable;

/**
 * This is a simple class, which aggregates double values 
 * and counts the number of values added.
 *
 * @author Mahmoud Parsian
 *
 */
public class MutableDouble implements Serializable {

      private double value = 0.0;
      private double count = 0.0;
      
      public MutableDouble(double d) {
         value = d;
         count = 1.0;
      }
      
      public MutableDouble(Double d) {
         if (d == null) {
            value = 0.0;
         }
         else {
            value = d;
         }
         count = 1.0;
      }
  
      public void increment(Double d) { 
         if (d == null) {
            // value does not change
         }
         else {
            value += d;
         }
         count++; 
      }
      
      public void increment(double d) { 
         count++;
         value += d;
      } 
    
      public double avg() { 
         return value/count; 
      }
}   
