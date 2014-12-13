package org.dataalgorithms.chap24.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class compares two Text objects.
 *
 * @author Mahmoud Parsian
 *
 */
public class BaseComparator extends WritableComparator {

	protected BaseComparator(){
		super(Text.class, true);
	}
	
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
	   Text t1 = (Text) w1;
	   Text t2 = (Text) w2;
	   String s1 = t1.toString().toUpperCase();
	   String s2 = t2.toString().toUpperCase();
	   int cmp = s1.compareTo(s2);
	   return cmp;
    }
}
