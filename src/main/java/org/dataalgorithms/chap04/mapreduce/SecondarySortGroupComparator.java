package org.dataalgorithms.chap04.mapreduce;

import org.apache.hadoop.io.RawComparator;
import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.DataInputBuffer;


/** 
 * This is an plug-in class.
 * The SecondarySortGroupComparator class indicates how to compare the userIDs.
 * 
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortGroupComparator 
	implements RawComparator<PairOfStrings> {

    /**
     *  Group only by userID
     */
    @Override
    public int compare(PairOfStrings first, PairOfStrings second) {
       return first.getLeftElement().compareTo(second.getLeftElement());
    }
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 ) {
    	DataInputBuffer buffer = new DataInputBuffer();
    	PairOfStrings a = new PairOfStrings();
    	PairOfStrings b = new PairOfStrings();
      	try {
        	buffer.reset(b1, s1, l1);
        	a.readFields(buffer);
        	buffer.reset(b2, s2, l2);
        	b.readFields(buffer);
        	return compare(a,b);  
      	} 
      	catch(Exception ex) {
        	return -1;
      	}  
    }
}
