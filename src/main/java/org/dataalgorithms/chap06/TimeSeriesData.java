package org.dataalgorithms.chap06;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.Writable;

import org.dataalgorithms.util.DateUtil;

/**
 * 
 * TimeSeriesData represents a pair of 
 *  (time-series-timestamp, time-series-value).
 *  
 * @author Mahmoud Parsian
 *
 */
public class TimeSeriesData 
   implements Writable, Comparable<TimeSeriesData> {

	private long timestamp;
	private double value;
	
	public static TimeSeriesData copy(TimeSeriesData tsd) {
		return new TimeSeriesData(tsd.timestamp, tsd.value);
	}
	
	public TimeSeriesData(long timestamp, double value) {
		set(timestamp, value);
	}
	
	public TimeSeriesData() {
	}
	
	public void set(long timestamp, double value) {
		this.timestamp = timestamp;
		this.value = value;
	}	
	
	public long getTimestamp() {
		return this.timestamp;
	}
	
	public double getValue() {
		return this.value;
	}
	
	/**
	 * Deserializes the point from the underlying data.
	 * @param in a DataInput object to read the point from.
	 */
	public void readFields(DataInput in) throws IOException {
		this.timestamp  = in.readLong();
		this.value  = in.readDouble();
	}

	/**
	 * Convert a binary data into TimeSeriesData
	 * 
	 * @param in A DataInput object to read from.
	 * @return A TimeSeriesData object
	 * @throws IOException
	 */
	public static TimeSeriesData read(DataInput in) throws IOException {
		TimeSeriesData tsData = new TimeSeriesData();
		tsData.readFields(in);
		return tsData;
	}

	public String getDate() {
		return DateUtil.getDateAsString(this.timestamp);	
	}

   /**
    * Creates a clone of this object
    */
    public TimeSeriesData clone() {
       return new TimeSeriesData(timestamp, value);
    }

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.timestamp );
		out.writeDouble(this.value );

	}

	/**
	 * Used in sorting the data in the reducer
	 */
	@Override
	public int compareTo(TimeSeriesData data) {
		if (this.timestamp  < data.timestamp ) {
			return -1;
		} 
		else if (this.timestamp  > data.timestamp ) {
			return 1;
		}
		else {
		   return 0;
		}
	}
	
	public String toString() {
       return "("+timestamp+","+value+")";
    }
}
