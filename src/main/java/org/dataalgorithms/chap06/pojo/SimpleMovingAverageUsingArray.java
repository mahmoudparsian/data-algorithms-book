package org.dataalgorithms.chap06.pojo;

/** 
 * Simple moving average by using an array data structure.
 *
 * @author Mahmoud Parsian
 *
 */
public class SimpleMovingAverageUsingArray {

    private double sum = 0.0;
    private final int period;
    private double[] window = null;
    private int pointer = 0;
    private int size = 0;
 
    public SimpleMovingAverageUsingArray(int period) {
        if (period < 1) {
           throw new IllegalArgumentException("period must be > 0");
        }
        this.period = period;
        window = new double[period];
    }
 
    public void addNewNumber(double number) {
        sum += number;
        if (size < period) {
            window[pointer++] = number;
            size++;
        }
        else {
            // size = period (size cannot be > period)
            pointer = pointer % period;
            sum -=  window[pointer];
            window[pointer++] = number;
        }
    }
 
    public double getMovingAverage() {
        if (size == 0) {
            throw new IllegalArgumentException("average is undefined");
        }
        return sum / size;
    }
}
