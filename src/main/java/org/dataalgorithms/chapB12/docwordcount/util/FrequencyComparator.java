package org.dataalgorithms.chapB12.docwordcount.util;

import scala.Tuple2;
import java.util.Comparator;
import java.io.Serializable;

/**
 * The FrequencyComparator class enable us to compare 
 * two Tuple2<Integer, String> objects based on the 
 * first Tuple2 argument.
 *
 * @author Mahmoud Parsian
 *
 */
public class FrequencyComparator
        implements Comparator<Tuple2<Integer, String>>, Serializable {

    public static final FrequencyComparator INSTANCE = new FrequencyComparator();

    @Override
    public int compare(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
        return t1._1.compareTo(t2._1);
    }
}
