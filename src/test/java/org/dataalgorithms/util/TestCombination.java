package org.dataalgorithms.util;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * This class tests the Combination class, which finds 
 * unique combinations of a given collection of objects.
 *
 * @author Mahmoud Parsian
 *
 */
public class TestCombination {
	
	public static void main(String[] args) throws Exception {
		test1();
		System.out.println("==========");
		test2();
		System.out.println("==========");
		test3();
		System.out.println("==========");
		System.exit(0);
	}
	
	public static void test3() throws Exception {
		List<String> list = Arrays.asList("a", "b", "c", "d");    
		System.out.println("list="+list);	
		System.out.println("==========");		
		List<List<String>> comb = Combination.findSortedCombinations(list);
		System.out.println(comb.size());
		System.out.println(comb);	
	}

	
	public static void test2() throws Exception {
		List<ImmutablePair<String, Integer>> list = new ArrayList<ImmutablePair<String, Integer>>();
		list.add(new ImmutablePair<String, Integer>("d1", 1));
		list.add(new ImmutablePair<String, Integer>("d2", 1));
		list.add(new ImmutablePair<String, Integer>("d3", 1));
		List<List<ImmutablePair<String, Integer>>> comb = Combination.findSortedCombinations(list, 2);
		System.out.println(comb.size());
		System.out.println(comb);
		for (List<ImmutablePair<String, Integer>> l : comb) {
			System.out.println(l);
		}
	}
	
		
	public static void test1() throws Exception {
		List<String> list = Arrays.asList("butter", "milk", "cracker");    
		System.out.println("list="+list);	

		List<List<String>> comb2 = Combination.findSortedCombinations(list, 2);
		System.out.println(comb2.size());
		System.out.println(comb2);
		for (List<String> l2 : comb2) {
			System.out.println(l2);
		}
		System.out.println("==========");
		
		List<List<String>> comb = Combination.findSortedCombinations(list);
		System.out.println(comb.size());
		System.out.println(comb);	
	}
}
