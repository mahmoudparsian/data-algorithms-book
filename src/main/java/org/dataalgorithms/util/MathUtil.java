package org.dataalgorithms.util;

import java.util.List;
//
import org.apache.commons.math.stat.inference.TTest;
import org.apache.commons.math.stat.inference.TTestImpl;

/**
 * MathUtil is a utility class, which provides some basic 
 * math. and statistical functions (such as ttest).
 *
 * @author Mahmoud Parsian
 *
 */
public class MathUtil {

    private static final TTest ttest = new TTestImpl();

    public static double ttest(double[] arrA, double[] arrB) {
        if ((arrA.length == 1) && (arrB.length == 1)) {
            // return a NULL value for score (does not make sense)
            return Double.NaN;
        }

        try {
            if (arrA.length == 1) {
                return ttest.tTest(arrA[0], arrB);
            } 
            else if (arrB.length == 1) {
                return ttest.tTest(arrB[0], arrA);
            } 
            else {
                return ttest.tTest(arrA, arrB);
            }
        } 
        catch (Exception e) {
            e.printStackTrace();
            return Double.NaN;
        }
    }

    public static double ttest(List<Double> groupA, List<Double> groupB) {
        if ((groupA.size() == 1) && (groupB.size() == 1)) {
            return Double.NaN;
        }

        if (groupA.size() == 1) {
            return tTest(groupA.get(0), groupB);
        } 
        else if (groupB.size() == 1) {
            return tTest(groupB.get(0), groupA);
        } 
        else {
            return tTest(groupA, groupB);
        }
    }

    private static double tTest(double d, List<Double> group) {
        try {
            double[] arr = listToArray(group);
            return ttest.tTest(d, arr);
        } 
        catch (Exception e) {
            e.printStackTrace();
            return Double.NaN;
        }
    }

    private static double tTest(List<Double> groupA, List<Double> groupB) {
        try {
            double[] arrA = listToArray(groupA);
            double[] arrB = listToArray(groupB);
            return ttest.tTest(arrA, arrB);
        } 
        catch (Exception e) {
            e.printStackTrace();
            return 0.0d;
        }
    }

    static double[] listToArray(List<Double> list) {
        if ((list == null) || (list.isEmpty())) {
            return null;
        }

        double[] arr = new double[list.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }

}
