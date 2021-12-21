package org.dataalgorithms.chap23.correlation;

import java.util.List;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

/**
 * Class for calculating Spearman's Rank Correlation between two vectors.
 * 
 * @author Mahmoud Parsian
 *
 */
public class Spearman {

    final static SpearmansCorrelation SC = new SpearmansCorrelation();

    public static double getCorrelation(List<Double> X, List<Double> Y) {
       double[] xArray = toDoubleArray(X);
       double[] yArray = toDoubleArray(Y);
       double corr = SC.correlation(xArray, yArray);
       return corr;
    }
    
    public static double getPvalue(final double corr, final int n) {
       return getPvalue(corr, (double) n);
    }
    
    public static double getPvalue(double corr, double n) {
       double t = Math.abs(corr * Math.sqrt( (n-2.0) / (1.0 - (corr * corr)) ));
       System.out.println("     t = "+ t);	
       TDistribution tdist = new TDistribution(n-2);
       double pvalue = 2.0 * (1.0 - tdist.cumulativeProbability(t));	// p-value worked. 		
       return pvalue;
    }

    public static double[] toDoubleArray(List<Double> list) {
       double[] arr = new double[list.size()];
       for (int i=0; i < list.size(); i++) {
          arr[i] = list.get(i);
       }
       return arr;
    }
}
