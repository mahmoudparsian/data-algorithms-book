package org.dataalgorithms.chap01.mapreduce;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** 
 * The DateTemperaturePartitioner is a custom partitioner class,
 * whcih partitions data by the natural key only (using the yearMonth).
 * Without custom partitioner, Hadoop will partition your mapped data 
 * based on a hash code.
 *
 * In Hadoop, the partitioning phase takes place after the map() phase 
 * and before the reduce() phase
 *
 * @author Mahmoud Parsian
 *
 */
public class DateTemperaturePartitioner 
   extends Partitioner<DateTemperaturePair, Text> {

    @Override
    public int getPartition(DateTemperaturePair pair, 
                            Text text, 
                            int numberOfPartitions) {
    	// make sure that partitions are non-negative
        return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
    }
}
