package org.dataalgorithms.util;

import java.util.SortedMap;

/**
 * A utility class to do basic data structure operations...
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class DataStructures {
        
    /**
     * Merge smaller Map into a larger Map
     * @param smaller a Map
     * @param larger a Map
     * @return merged elements
     */
    public static SortedMap<Integer, Integer> merge(
            final SortedMap<Integer, Integer> smaller, 
            final SortedMap<Integer, Integer> larger) {
        //
        for (Integer key : smaller.keySet()) {
            Integer valueFromLargeMap = larger.get(key);
            if (valueFromLargeMap == null) {
                larger.put(key, smaller.get(key));
            } 
            else {
                int mergedValue = valueFromLargeMap + smaller.get(key);
                larger.put(key, mergedValue);
            }
        }
        //
        return larger;
    }    
    
}
