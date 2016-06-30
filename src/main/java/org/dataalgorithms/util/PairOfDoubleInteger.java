package org.dataalgorithms.util;

/**
 * PairOfDoubleInteger represents a pair of (double, integer).
 *
 * @author Mahmoud Parsian
 *
 */
public class PairOfDoubleInteger {

    private double value = 0.0;
    private int count = 0;

    public PairOfDoubleInteger(double value) {
        this.value = value;
        this.count = 1;
    }

    public PairOfDoubleInteger(double value, int count) {
        this.value = value;
        this.count = count;
    }

    public void increment(double value) {
        // increment is by 1 as default
        this.value += value;
        this.count++;
    }

    public void increment(double value, int count) {
        // increment is by 1 as default
        this.value += value;
        this.count += count;
    }

    public double getValue() {
        return value;
    }

    public int getCount() {
        return count;
    }

    public double avg() {
        if (count == 0) {
            if ((value == 0) || (value > 0.0)) {
                return Double.POSITIVE_INFINITY;
            } 
            else {
                return Double.NEGATIVE_INFINITY;
            }
        }
        return value / ((double) count);
    }
}
