package org.dataalgorithms.chap01.spark;

import java.io.Serializable;
//
import java.util.Comparator;
//
import scala.Tuple2;

/**
 * TupleComparatorDescending: how to sort in descending order
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 * 
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class TupleComparatorDescending implements Serializable, Comparator<Tuple2<String, Integer>> {

    private static final long serialVersionUID = 1287049512718728895L;
    
    static final TupleComparatorDescending INSTANCE = new TupleComparatorDescending();

    private TupleComparatorDescending() {
    }
    
    
    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        if (o2._1.compareTo(o1._1) == 0) {
            return o2._2.compareTo(o1._2);
        } else {
            return o2._1.compareTo(o1._1);
        }
    }

}
