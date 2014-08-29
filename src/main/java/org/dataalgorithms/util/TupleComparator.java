package org.dataalgorithms.util;

import scala.Tuple2;
import java.util.Comparator;
 
/**
 * This class enables to compare two Tuple2<Integer, Integer> objects.
 * Used for sorting purposes. It just compares the first object 
 * from a given two Tuple2.
 *
 * @author Mahmoud Parsian
 *
 */
public class TupleComparator implements Comparator<Tuple2<Integer, Integer>> {
   public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
      return t1._1.compareTo(t2._1);
   }
}
