package org.dataalgorithms.chap11.projection.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * CompositeKey: represents a pair of 
 * (String customerID, long timestamp).
 * 
 * 
 * We do a primary grouping pass on the customerID field to get all  
 * of the data of one type together, and then our "secondary sort"  
 * during the shuffle phase uses the timestamp long member (representing  
 * the purchase-date) to sort the pairs of PairOfLongInt so that they
 * arrive at the reducer partitioned and in sorted order.
 *  
 * @author Mahmoud Parsian
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    // natural key is (customerID)
    // composite key is a pair (customerID, timestamp)
	private String customerID;
	private long timestamp;

	public CompositeKey(String customerID, long timestamp) {
		set(customerID, timestamp);
	}
	
	public CompositeKey() {
	}

	public void set(String customerID, long timestamp) {
		this.customerID = customerID;
		this.timestamp = timestamp;
	}

	public String getCustomerID() {
		return this.customerID;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.customerID = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.customerID);
		out.writeLong(this.timestamp);
	}

	@Override
	public int compareTo(CompositeKey other) {
		if (this.customerID.compareTo(other.customerID) != 0) {
			return this.customerID.compareTo(other.customerID);
		} 
		else if (this.timestamp != other.timestamp) {
			return timestamp < other.timestamp ? -1 : 1;
		} 
		else {
			return 0;
		}

	}

	public static class CompositeKeyComparator extends WritableComparator {
		public CompositeKeyComparator() {
			super(CompositeKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(CompositeKey.class,
				new CompositeKeyComparator());
	}

}
