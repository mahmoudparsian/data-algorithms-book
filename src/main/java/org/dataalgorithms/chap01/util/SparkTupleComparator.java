package org.dataalgorithms.chap01.util;


import scala.Tuple2;
import java.util.Comparator;
import java.io.Serializable;

/** 
 * The SparkTupleComparator class enable us to compare two 
 * Tuple2<Integer, Integer> objects based on the first Tuple2
 * argument.
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkTupleComparator 
   implements Comparator<Tuple2<Integer, Integer>>, Serializable {
    
   public static final SparkTupleComparator INSTANCE = new SparkTupleComparator();
   
   private SparkTupleComparator() {
   }
   
   @Override
   public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2){
      return t1._1.compareTo(t2._1);
   }
}
