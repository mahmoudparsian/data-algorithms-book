package org.dataalgorithms.chapB14.minmax.spark;

import scala.Tuple2;
//
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Description:
 *
 * Util: Find minimum and maximum of a set of numbers from a given List<Tuple2<min.max>>
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    /**
     * Convert a pair of (min, max) to Iterator<Tuple2<min, max>>
     *
     * @param min
     * @param max
     * 
     */
    public static Iterator<Tuple2<Integer, Integer>> toIterator(int min, int max) {
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(min, max));
        return list.iterator();
    }

    /**
     * Find min and max from a list of Tuple2<min,max>
     * @param list of Tuple2<min,max>
     * @return Tuple2<min,max>
     * 
     */
    public static Tuple2<Integer, Integer> findMinMax(List<Tuple2<Integer, Integer>> list) {
        boolean firstTime = true;
        int min = 0; // 0 is never used
        int max = 0; // 0 in never used
        for (Tuple2<Integer, Integer> entry : list) {
            int localMin = entry._1;
            int localMax = entry._2;
            if (firstTime) {
                min = localMin;
                max = localMax;
                firstTime = false;
            } else {
                if (localMin < min) {
                    min = localMin;
                }
                if (localMax > max) {
                    max = localMax;
                }
            }
        }
        //
        return new Tuple2<>(min, max);
    }

}
