package org.dataalgorithms.chap23.correlation;

import java.util.Arrays;
import java.util.List;

/**
 * Test Class for calculating Spearman's Rank Correlation between two vectors.
 * 
 * @author Mahmoud Parsian
 *
 */
public class TestSpearman {
		
	public static void main(String[] args) {
	    test0(args);
	    test1(args);
	    test2(args);
	}
	
    public static void test0(String[] args) {
	    //                             1     2    3     4    5
		List<Double> X = Arrays.asList(2.0,  4.0, 45.0, 6.0, 7.0);
		List<Double> Y = Arrays.asList(23.0, 5.0, 54.0, 6.0, 7.0);
		double n = X.size(); // 5.0;
		double corr = Spearman.getCorrelation(X, Y);
		double pvalue = Spearman.getPvalue(corr, n);
		System.out.println("corr   = "+ corr);
		System.out.println("pvalue = "+ pvalue);

	    //                             1     2    3     4    5
		List<Double> X2 = Arrays.asList(12.0,  14.0, 45.0, 6.0, 17.0);
		List<Double> Y2 = Arrays.asList(3.0, 5.0, 15.0, 16.0, 17.0);
		double n2 = X2.size(); // 5.0;
		double corr2 = Spearman.getCorrelation(X2, Y2);
		double pvalue2 = Spearman.getPvalue(corr2, n2);
		System.out.println("corr2   = "+ corr2);
		System.out.println("pvalue2 = "+ pvalue2);
    }
    
	public static void test1(String[] args) {
	    //                             1     2    3     4    5
		List<Double> X = Arrays.asList(2.0,  4.0, 45.0, 6.0, 7.0);
		List<Double> Y = Arrays.asList(23.0, 5.0, 54.0, 6.0, 7.0);
		double n = 5.0;
		double corr = Spearman.getCorrelation(X, Y);
		double pvalue = Spearman.getPvalue(corr, n);
		System.out.println("corr   = "+ corr);
		System.out.println("pvalue = "+ pvalue);
	}
	
	public static void test2(String[] args) {
		List<Double> X = Arrays.asList(-2.23582587121604,-1.85296859915777,-1.2984667458879,4.33400363124673,1.12600971492544,-1.05543924472576,1.28728316963258);
		// (91775r2,1.0,1.0,1.0,1.0,-1.0,-1.0,1.0,-2.813988095238096,NaN)
		List<Double> Y = Arrays.asList(1.0,1.0,1.0,1.0,-1.0,-1.0,1.0);
		double n = 7.0;
		double corr = Spearman.getCorrelation(X, Y);
		double pvalue = Spearman.getPvalue(corr, n);
		System.out.println("corr   = "+ corr);
		System.out.println("pvalue = "+ pvalue);
	}

}

/*
> x=c(2,4,45,6,7)
> y=c(23,5,54,6,7)
> cor.test(x,y)

	Pearson's product-moment correlation

data:  x and y 
t = 3.6026, df = 3, p-value = 0.03669
alternative hypothesis: true correlation is not equal to 0 
95 percent confidence interval:
 0.09266873 0.99352355 
sample estimates:
      cor 
0.9012503 

*/
