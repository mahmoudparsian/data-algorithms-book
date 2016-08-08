package org.dataalgorithms.chap06.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * CompositeKey: represents a pair of 
 * (String name, long timestamp).
 * 
 * 
 * We do a primary grouping pass on the name field to get all of the data of
 * one type together, and then our "secondary sort" during the shuffle phase
 * uses the timestamp long member to sort the timeseries points so that they
 * arrive at the reducer partitioned and in sorted order.
 * 
 * 
 * @author Mahmoud Parsian
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    // natural key is (name)
    // composite key is a pair (name, timestamp)
	private String name;
	private long timestamp;

	public CompositeKey(String name, long timestamp) {
		set(name, timestamp);
	}
	
	public CompositeKey() {
	}

	public void set(String name, long timestamp) {
		this.name = name;
		this.timestamp = timestamp;
	}

	public String getName() {
		return this.name;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeLong(this.timestamp);
	}

	@Override
	public int compareTo(CompositeKey other) {
		if (this.name.compareTo(other.name) != 0) {
			return this.name.compareTo(other.name);
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
