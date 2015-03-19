package org.dataalgorithms.chap06.pojo;

import java.util.Queue;
import java.util.LinkedList;

/** 
 * Simple moving average by using a queue data structure.
 *
 * @author Mahmoud Parsian
 *
 */
public class SimpleMovingAverage {

    private double sum = 0.0;
    private final int period;
    private final Queue<Double> window = new LinkedList<Double>();
 
    public SimpleMovingAverage(int period) {
        if (period < 1) {
           throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
    }
 
    public void addNewNumber(double number) {
        sum += number;
        window.add(number);
        if (window.size() > period) {
            sum -= window.remove();
        }
    }
 
    public double getMovingAverage() {
        if (window.isEmpty()) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / window.size();
    }
}
