package org.dataalgorithms.chap23.correlation;

import java.util.List;
import java.util.Arrays;

/**
 * This Test Class for the Pearson Correlation Coefficient
 * and its associated p-value.
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class TestPearson {
  
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
      double corr = Pearson.getCorrelation(X, Y);
      double pvalue = Pearson.getPvalue(corr, n);
      System.out.println("corr   = "+ corr);
      System.out.println("pvalue = "+ pvalue);

      //                             1     2    3     4    5
      List<Double> X2 = Arrays.asList(12.0,  14.0, 45.0, 6.0, 17.0);
      List<Double> Y2 = Arrays.asList(3.0, 5.0, 15.0, 16.0, 17.0);
      double n2 = X2.size(); // 5.0;
      double corr2 = Pearson.getCorrelation(X2, Y2);
      double pvalue2 = Pearson.getPvalue(corr2, n2);
      System.out.println("corr2   = "+ corr2);
      System.out.println("pvalue2 = "+ pvalue2);
   }
   
   public static void test1(String[] args) {
            //                             1     2    3     4    5
            List<Double> X = Arrays.asList(2.0,  4.0, 45.0, 6.0, 7.0);
            List<Double> Y = Arrays.asList(23.0, 5.0, 54.0, 6.0, 7.0);
            double n = 5.0;
            double corr = Pearson.getCorrelation(X, Y);
            double pvalue = Pearson.getPvalue(corr, n);
            System.out.println("corr   = "+ corr);
            System.out.println("pvalue = "+ pvalue);
   }
   public static void test2(String[] args) {
            // 37761r2
            List<Double> X = Arrays.asList(-2.23582587121604,-1.85296859915777,-1.2984667458879,4.33400363124673,1.12600971492544,-1.05543924472576,1.28728316963258);
            // 87043r2
            List<Double> Y = Arrays.asList(-7.52686462600734,-8.30573303914744,5.51558960484512,-10.1470117322862,-8.7196434139086,-23.5494328102242,-9.32157436441666);
            double n = 7.0;
            double corr = Pearson.getCorrelation(X, Y);
            double pvalue = Pearson.getPvalue(corr, n);
            System.out.println("corr   = "+ corr);
            System.out.println("pvalue = "+ pvalue);
            //expected corr   = -0.11125212554217496
            //expected pvalue =  0.8122991623071378)   
    }
    
    public static double[] toDoubleArray(List<Double> list) {
         double[] arr = new double[list.size()];
         for (int i=0; i < list.size(); i++) {
            arr[i] = list.get(i);
         }
         return arr;
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

x = c(-2.23582587121604,-1.85296859915777,-1.2984667458879,4.33400363124673,1.12600971492544,-1.05543924472576,1.28728316963258)
y = c(-7.52686462600734,-8.30573303914744,5.51558960484512,-10.1470117322862,-8.7196434139086,-23.5494328102242,-9.32157436441666)
cor.test(x,y)
      Pearson's product-moment correlation

data:  x and y 
t = -0.2503, df = 5, p-value = 0.8123
alternative hypothesis: true correlation is not equal to 0 
95 percent confidence interval:
 -0.7974965  0.7004928 
sample estimates:
       cor 
-0.1112521 

# java org/dataalgorithms/chap26.Pearson
     t = 3.602619932479022
corr   = 0.9012502759005416
pvalue = 0.03669423980590647
     t = 0.5056784001383844
corr2   = 0.2802537958851099
pvalue2 = 0.6478974587092594


*/

