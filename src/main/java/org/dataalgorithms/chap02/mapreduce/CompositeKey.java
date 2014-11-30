package org.dataalgorithms.chap02.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * CompositeKey: represents a pair of 
 * (String stockSymbol, long timestamp).
 * 
 * 
 * We do a primary grouping pass on the stockSymbol field to get 
 * all of the data of one type together, and then our "secondary sort" 
 * during the shuffle phase uses the timestamp long member to sort 
 * the timeseries points so that they arrive at the reducer partitioned 
 * and in sorted order.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    // natural key is (stockSymbol)
    // composite key is a pair (stockSymbol, timestamp)
	private String stockSymbol;
	private long timestamp;

	public CompositeKey(String stockSymbol, long timestamp) {
		set(stockSymbol, timestamp);
	}
	
	public CompositeKey() {
	}

	public void set(String stockSymbol, long timestamp) {
		this.stockSymbol = stockSymbol;
		this.timestamp = timestamp;
	}

	public String getStockSymbol() {
		return this.stockSymbol           ;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stockSymbol = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.stockSymbol);
		out.writeLong(this.timestamp);
	}

	@Override
	public int compareTo(CompositeKey other) {
		if (this.stockSymbol.compareTo(other.stockSymbol) != 0) {
			return this.stockSymbol.compareTo(other.stockSymbol);
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
