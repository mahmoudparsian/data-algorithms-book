package org.dataalgorithms.machinelearning.kmeans;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
//
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.DenseVector;
//
import org.apache.commons.lang.StringUtils;

/**
 * Provide common methods for K-Means algorithms.
 *
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class Util {

    static double squaredDistance(Vector a, Vector b) {
        double distance = 0.0;
        int size = a.size();
        for (int i = 0; i < size; i++) {
            double diff = a.apply(i) - b.apply(i);
            distance += diff * diff;
        }
        return distance;
    }

    /**
     * Build a Vector from given features denoted by a comma separated feature values
     *
     * @param features set of features in the format:
     * <feature_1><,><feature_2><,>...<,><feature_N>
     * @param delimiter such as ",", "\t", ";", ...
     * @return a Vector of features
     */
    static Vector buildVector(final String features, final String delimiter) {
        String[] tokens = StringUtils.split(features, delimiter);
        double[] d = new double[tokens.length];
        for (int i = 0; i < d.length; i++) {
            d[i] = Double.parseDouble(tokens[i]);
        }
        return new DenseVector(d);
    }

    static int closestPoint(Vector p, List<Vector> centers) {
        int bestIndex = 0;
        double closest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centers.size(); i++) {
            double tempDist = squaredDistance(p, centers.get(i));
            if (tempDist < closest) {
                closest = tempDist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    static Vector average(List<Vector> list) {
        // find sum
        double[] sum = new double[list.get(0).size()];
        for (Vector v : list) {
            for (int i = 0; i < sum.length; i++) {
                sum[i] += v.apply(i);
            }
        }

        // find averages...
        int numOfVectors = list.size();
        for (int i = 0; i < sum.length; i++) {
            sum[i] = sum[i] / numOfVectors;
        }
        return new DenseVector(sum);
    }

    static Vector average(Iterable<Vector> ps) {
        List<Vector> list = new ArrayList<Vector>();
        for (Vector v : ps) {
            list.add(v);
        }
        return average(list);
    }
    
    static Vector add(Vector a, Vector b) {
        double[] sum = new double[a.size()];
        for (int i = 0; i < sum.length; i++) {
            sum[i] += a.apply(i) + b.apply(i);
        }
        return new DenseVector(sum);
    }


    static double getDistance(final List<Vector> centroids, final Map<Integer, Vector> newCentroids, final int K) {
        double distance = 0.0;
        for (int i = 0; i < K; i++) {
            if (centroids.get(i) != null && newCentroids.get(i) != null) {
                distance += Util.squaredDistance(centroids.get(i), newCentroids.get(i));
            }
        }
        distance = distance / K;
        return distance;
    }

}
