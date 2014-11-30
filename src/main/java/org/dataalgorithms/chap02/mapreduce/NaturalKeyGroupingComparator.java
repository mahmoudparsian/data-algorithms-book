package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * NaturalKeyGroupingComparator
 * 
 * This class is used during Hadoop's shuffle phase to group 
 * composite key's by the first part (natural) of their key.
 * The natural key for time series data is the "name".
 * 
 *  @author Mahmoud Parsian 
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

	protected NaturalKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		CompositeKey ck1 = (CompositeKey) wc1;
		CompositeKey ck2 = (CompositeKey) wc2;
		return ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
	}

}
