package org.dataalgorithms.chap06.pojo;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

/** 
 * Basic testing of Simple moving average.
 *
 * @author Mahmoud Parsian
 *
 */
public class TestSimpleMovingAverage { 

    private static final Logger THE_LOGGER = Logger.getLogger(TestSimpleMovingAverage.class);

    public static void main(String[] args) {
        // The invocation of the BasicConfigurator.configure method 
        // creates a rather simple log4j setup. This method is hardwired 
        // to add to the root logger a ConsoleAppender.
        BasicConfigurator.configure();
        
        // time series        1   2   3  4   5   6   7
        double[] testData = {10, 18, 20, 30, 24, 33, 27};
        int[] allWindowSizes = {3, 4};
        for (int windowSize : allWindowSizes) {
            SimpleMovingAverage sma = new SimpleMovingAverage(windowSize);
            THE_LOGGER.info("windowSize = " + windowSize);
            for (double x : testData) {
                sma.addNewNumber(x);
                THE_LOGGER.info("Next number = " + x + ", SMA = " + sma.getMovingAverage());
            }
            THE_LOGGER.info("---");
        }
    }
}
