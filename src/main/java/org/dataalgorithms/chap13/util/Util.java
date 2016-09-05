package org.dataalgorithms.chap13.util;


// STEP-0: Import required classes and interfaces
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
//
import scala.Tuple2;
//
import com.google.common.base.Splitter;

/**
 * common functions for using with and without Lambda
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {
   
   /**
    * @param str a comma (or semicolon) separated list of double values
    * str is like "1.1,2.2,3.3" or "1.1;2.2;3.3"
    *
    * @param delimiter a delimiter such as ",", ";", ...
    * @return a List<Long> 
    */
    public static List<Double> splitOnToListOfDouble(String str, String delimiter) {
       Splitter splitter = Splitter.on(delimiter).trimResults();
       Iterable<String> tokens = splitter.split(str);
       if (tokens == null) {
          return null;
       }
       List<Double> list = new ArrayList<Double>();
       for (String token: tokens) {
         double data = Double.parseDouble(token);
         list.add(data);
       }
       return list;
    }   
   
   /**
    * @param rAsString = "r.1,r.2,...,r.d"
    * @param sAsString = "s.1,s.2,...,s.d"
    * @param d dimension of R and S
    */
   public static double calculateDistance(String rAsString, String sAsString, int d) {
      List<Double> r = splitOnToListOfDouble(rAsString, ",");
      List<Double> s = splitOnToListOfDouble(sAsString, ",");
 
      // d is the number of dimensions in the vector 
      if (r.size() != d) {
         return Double.NaN;
      }
      //
      if (s.size() != d) {
         return Double.NaN;
      }      
      
      // here we have (r.size() == s.size() == d) 
      double sum = 0.0;
      for (int i = 0; i < d; i++) {
        double difference = r.get(i) - s.get(i);
        sum += difference * difference;
      }
      //
      return Math.sqrt(sum);
   }
   
   /**
    * @param rAsString = "r.1,r.2,...,r.d"
    * @param sAsString = "s.1,s.2,...,s.d"
    * @param d dimension of R and S
    */
   public static double calculateDistance(double[] r, double[] s, int d) { 
      // d is the number of dimensions in the vector 
      if (r.length != d) {
         return Double.NaN;
      }
      //
      if (s.length != d) {
         return Double.NaN;
      }      
      
      // here we have (r.length == s.length == d) 
      double sum = 0.0;
      for (int i = 0; i < d; i++) {
        double difference = r[i] - s[i];
        sum += difference * difference;
      }
      //
      return Math.sqrt(sum);
   }   

   public static SortedMap<Double, String> findNearestK(Iterable<Tuple2<Double,String>> neighbors, int k) {
       // keep only k-nearest-neighbors
       SortedMap<Double, String>  nearestK = new TreeMap<Double, String>();
       for (Tuple2<Double,String> neighbor : neighbors) {
          Double distance = neighbor._1;
          String classificationID =  neighbor._2;
          nearestK.put(distance, classificationID);
          // keep only k-nearest-neighbors
          if (nearestK.size() > k) {
             // remove the last highest distance neighbor from nearestK
             nearestK.remove(nearestK.lastKey());
          }      
       }
       return nearestK;
   }
 
   public static Map<String, Integer> buildClassificationCount(Map<Double, String> nearestK) {
       Map<String, Integer> majority = new HashMap<String, Integer>();
       for (Map.Entry<Double, String> entry : nearestK.entrySet()) {
          String classificationID = entry.getValue();
          Integer count = majority.get(classificationID);
          if (count == null){
             majority.put(classificationID, 1);
          }
          else {
             majority.put(classificationID, count+1);
          }
       } 
       return majority;
   }        

   public static String classifyByMajority(Map<String, Integer> majority) {
     int votes = 0;
     String selectedClassification = null;
     for (Map.Entry<String, Integer> entry : majority.entrySet()) {
        if (selectedClassification == null) {
            selectedClassification = entry.getKey();
            votes = entry.getValue();
        }
        else {
            int count = entry.getValue();
            if (count > votes) {
                selectedClassification = entry.getKey();
                votes = count;
            }
        }
     }
     return selectedClassification;
   }
   
}
