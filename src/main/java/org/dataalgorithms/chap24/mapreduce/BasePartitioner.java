package org.dataalgorithms.chap24.mapreduce;


import org.apache.hadoop.mapreduce.Partitioner;

/** 
 * This is a custom partitioner.
 * Partition keys by bases{A,T,G,C,a,t,g,c}. 
 * 
 * @author Mahmoud Parsian
 *
 */
public class BasePartitioner<K, V> extends Partitioner<K, V> {

  public int getPartition(K key, V value, int numReduceTasks) {
	 String base = key.toString();
	 if (base.compareToIgnoreCase("A") == 0){
		return 0;
	 }
	 else if (base.compareToIgnoreCase("C") == 0){
		return 1;
	 } 
	 else if (base.compareToIgnoreCase("G") == 0){
        return 2;
	 }
	 else if(base.compareToIgnoreCase("T") == 0){
        return 3;
	 }
	 else{
		return 4;
	 }
  }


}
