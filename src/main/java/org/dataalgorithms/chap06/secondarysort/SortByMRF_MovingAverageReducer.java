package org.dataalgorithms.chap06.secondarysort;

import java.util.Iterator;
import java.io.IOException;
//
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
//
import org.dataalgorithms.util.DateUtil;
import org.dataalgorithms.chap06.TimeSeriesData;

/**
 * 
 * SortByMRF_MovingAverageReducer implements the reduce() function.
 * 
 * Data arrive sorted to reducer (reducers values are sorted by 
 * MapReduce framework  -- NOTE: values are not sorted in memory).
 *  
 * @author Mahmoud Parsian
 *
 */ 
public class SortByMRF_MovingAverageReducer extends MapReduceBase
        implements Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    int windowSize = 5; // default window size

    /**
     * will be run only once get parameters from Hadoop's configuration
     */
    @Override
    public void configure(JobConf jobconf) {
        this.windowSize = jobconf.getInt("moving.average.window.size", 5);
    }

    @Override
    public void reduce(CompositeKey key,
            Iterator<TimeSeriesData> values,
            OutputCollector<Text, Text> output,
            Reporter reporter)
            throws IOException {

        // note that values are sorted.
        // apply moving average algorithm to sorted timeseries
        Text outputKey = new Text();
        Text outputValue = new Text();
        MovingAverage ma = new MovingAverage(this.windowSize);
        while (values.hasNext()) {
            TimeSeriesData data = values.next();
            ma.addNewNumber(data.getValue());
            double movingAverage = ma.getMovingAverage();
            long timestamp = data.getTimestamp();
            String dateAsString = DateUtil.getDateAsString(timestamp);
            //THE_LOGGER.info("Next number = " + x + ", SMA = " + sma.getMovingAverage());
            outputValue.set(dateAsString + "," + movingAverage);
            outputKey.set(key.getName());
            output.collect(outputKey, outputValue);
        }
        //
    } 

}
