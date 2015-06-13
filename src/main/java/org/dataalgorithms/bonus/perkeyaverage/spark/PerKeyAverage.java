package org.dataalgorithms.bonus.perkeyaverage.spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * This class finds "per key average" for all keys.
 * 
 * @author Mahmoud Parsian
 */

public final class PerKeyAverage {

    public static class AvgCount implements java.io.Serializable {
        double total;
        int num;

        public AvgCount(double total, int num) {
            this.total = total;
            this.num = num;
        }

        public double avg() {
            return total / (double) num;
        }
    }

    public static void main(String[] args) throws Exception {

        JavaSparkContext context = new JavaSparkContext();

        List<Tuple2<String, Double>> input = new ArrayList<Tuple2<String, Double>>();
        input.add(new Tuple2("coffee", 1.0));
        input.add(new Tuple2("coffee", 2.0));
        input.add(new Tuple2("coffee", 9.0));
        input.add(new Tuple2("pandas", 3.0));
        input.add(new Tuple2("pandas", 17.0));
        input.add(new Tuple2("duck", 2.0));
        input.add(new Tuple2("duck", 8.0));
        input.add(new Tuple2("duck", 4.0));
        input.add(new Tuple2("duck", 6.0));        
        JavaPairRDD<String, Double> rdd = context.parallelizePairs(input);
        
        // to use combineByKey(), you need to define 3 basic functions f1, f2, f3:
        // and then you invoke it as: combineByKey(f1, f2, f3)
        
        // function 1: 
        Function<Double, AvgCount> createAcc = new Function<Double, AvgCount>() {
            @Override
            public AvgCount call(Double x) {
                return new AvgCount(x, 1);
            }
        };
        
        // function 2
        Function2<AvgCount, Double, AvgCount> addAndCount = new Function2<AvgCount, Double, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Double x) {
                a.total += x;
                a.num += 1;
                return a;
            }
        };
        
        // function 3
        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
        
        AvgCount initial = new AvgCount(0, 0);
        // now that we have defined 3 functions, we can use combineByKey()
        JavaPairRDD<String, AvgCount> avgCounts = rdd.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
        
        // done
        
    }
}
