package org.dataalgorithms.util;

import java.util.SortedMap;

/**
 * A utility class to do basics.
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
            Integer value = larger.get(key);
            if (value == null) {
                larger.put(key, value);
            } 
            else {
                larger.put(key, value + smaller.get(key));
            }
        }
        //
        return larger;
    }    
    
}
