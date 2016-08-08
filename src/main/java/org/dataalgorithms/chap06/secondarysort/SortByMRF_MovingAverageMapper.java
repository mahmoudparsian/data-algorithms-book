package org.dataalgorithms.chap06.secondarysort;

import java.util.Date;
import java.io.IOException;
//
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.lang.StringUtils;
//
import org.dataalgorithms.util.DateUtil;
import org.dataalgorithms.chap06.TimeSeriesData;

/**
 * 
 * SortByMRF_MovingAverageMapper implements the map() function.
 *  
 * @author Mahmoud Parsian
 *
 */ 
public class SortByMRF_MovingAverageMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, CompositeKey, TimeSeriesData> {

    // reuse Hadoop's Writable objects
    private final CompositeKey reducerKey = new CompositeKey();
    private final TimeSeriesData reducerValue = new TimeSeriesData();

    @Override
    public void map(LongWritable inkey, Text value,
            OutputCollector<CompositeKey, TimeSeriesData> output,
            Reporter reporter) throws IOException {
        String record = value.toString();
        if ((record == null) || (record.length() == 0)) {
            return;
        }
        String[] tokens = StringUtils.split(record, ",");
        if (tokens.length == 3) {
            // tokens[0] = name of timeseries as string
            // tokens[1] = timestamp
            // tokens[2] = value of timeseries as double
            Date date = DateUtil.getDate(tokens[1]);
            if (date == null) {
                return;
            }
            long timestamp = date.getTime();
            reducerKey.set(tokens[0], timestamp);
            reducerValue.set(timestamp, Double.parseDouble(tokens[2]));
            // emit key-value pair
            output.collect(reducerKey, reducerValue);
        } 
        else {
            // log as error, not enough tokens
        }
    }
}
