package org.dataalgorithms.util;

import scala.Tuple2;
import java.util.Comparator;
import java.io.Serializable;

/**
 * This class enables to compare two Tuple2<Integer, Integer> objects.
 * Used for sorting purposes. It just compares the Tuple2's first elements. 
 *
 * @author Mahmoud Parsian
 *
 */
public class TupleComparator 
   implements Comparator<Tuple2<Integer, Integer>>, Serializable {
   @Override
   public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
      return t1._1.compareTo(t2._1);
   }
}
