/**
 * This package (org.dataalgorithms.chap03.mapreduce) contains source code  
 * for chapter 03 of the Data Algorithms book published by O'Reilly.
 * 
 * PHASE-1:
 *     AggregateByKeyDriver aggregates keys by values (generates unique (K,V)).
 *     for example, if you have (K, 2), (K, 4), and (K, 3) then it creates a 
 *     unique (K, 9) where (9=2+4+3).
 * 
 * PHASE-2:
 *     TopNDriver assumes that all K's are unique for all given inputs of (K,V).  
 *     This is a general solution, which finds top-N for all unique keys.
 *
 * @author Mahmoud Parsian
 *
 */
package org.dataalgorithms.chap03.mapreduce;

//rest of the file is empty
