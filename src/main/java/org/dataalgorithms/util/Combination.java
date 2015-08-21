package org.dataalgorithms.util;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Find unique combinations of a given collection of objects.
 *
 * @author Mahmoud Parsian
 *
 */
public class Combination {
   
    /**
     * Will return combinations of all sizes...
     * If elements = { a, b, c }, then findCollections(elements) 
     * will return all unique combinations of elements as:
     *
     *    { [], [a], [b], [c], [a, b], [a, c], [b, c], [a, b, c] }
     *
     * @param <T>
     * @param elements a collection of type T elements
     * @return unique combinations of elements
     */
    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = new ArrayList<List<T>>();
        for (int i = 0; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }
    
    
    /**
     * Will return unique combinations of size=n.
     * If elements = { a, b, c }, then findCollections(elements, 2) will return:
     *
     *     { [a, b], [a, c], [b, c] }
     *
     * @param <T>
     * @param elements a collection of type T elements
     * @param n size of combinations
     * @return unique combinations of elements of size = n
     *
     */
    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<List<T>>();
        
        if (n == 0) {
            result.add(new ArrayList<T>());
            return result;
        }
        
        List<List<T>> combinations = findSortedCombinations(elements, n - 1);
        for (List<T> combination: combinations) {
            for (T element: elements) {
                if (combination.contains(element)) {
                    continue;
                }
                
                List<T> list = new ArrayList<T>();
                list.addAll(combination);
                
                if (list.contains(element)) {
                    continue;
                }
                
                list.add(element);
                //sort items not to duplicate the items
                //   example: (a, b, c) and (a, c, b) might become  
                //   different items to be counted if not sorted   
                Collections.sort(list);
                
                if (result.contains(list)) {
                    continue;
                }
                
                result.add(list);
            }
        }
        
        return result;
    }
    
    /**
     * Basic Test of findSortedCombinations()
     * 
     * @param args 
     */
    public static void main(String[] args) {
        List<String> elements = Arrays.asList("a", "b", "c", "d", "e");
        List<List<String>> combinations = findSortedCombinations(elements, 2);
        System.out.println(combinations);
    }

}

