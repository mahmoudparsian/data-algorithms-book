package org.dataalgorithms.chap24.spark;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

/**
 *
 * Merge/combine HashMaps
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class Util {

    /**
     * Merge/combine a list of Map<Character, Long> into a single map
     * 
     * @param list a List<Map<Character, Long>>
     * @return merge of all Maps
     */
    public static Map<Character, Long> merge(List<Map<Character, Long>> list) {
        //
        if ((list == null) || (list.isEmpty())) {
            return null;
        }
        //
        Map<Character, Long> merged = new HashMap<>();
        //
        for (Map<Character, Long> map : list) {
            //
            if ((map == null) || (map.isEmpty())) {
                continue;
            }
            //
            for (Map.Entry<Character, Long> entry : map.entrySet()) {
                char base = entry.getKey();
                Long count = merged.get(base);
                if (count == null) {
                    merged.put(base, entry.getValue());
                } 
                else {
                    merged.put(base, (count + entry.getValue()));
                }
            }
        }
        //
        return merged;
    }

}
