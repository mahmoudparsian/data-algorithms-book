package org.dataalgorithms.chapB03.perkeyaverage.sparkwithlambda;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
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
    
    static final List<Tuple2<String, Double>> getSampleInput() {
        List<Tuple2<String, Double>> input = new ArrayList<Tuple2<String, Double>>();
        // key: "zebra"
        input.add(new Tuple2("zebra", 1.0));
        input.add(new Tuple2("zebra", 2.0));
        input.add(new Tuple2("zebra", 9.0));
        // key: "pandas"
        input.add(new Tuple2("pandas", 3.0));
        input.add(new Tuple2("pandas", 17.0));
        // key: "duck"
        input.add(new Tuple2("duck", 2.0));
        input.add(new Tuple2("duck", 8.0));
        input.add(new Tuple2("duck", 4.0));
        input.add(new Tuple2("duck", 6.0));
        return input;
    }

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

        // if you do not provide input, we will use a sample static input
        JavaPairRDD<String, Double> rdd = null;
        if (args.length > 0) {
            // use input provided 
            String inputPath = args[0];
            JavaRDD<String> recs = context.textFile(inputPath, 1);
            rdd = recs.mapToPair((String s) -> {
                String[] tokens = s.split(":");
                return new Tuple2<String,Double>(tokens[0], Double.parseDouble(tokens[1]));   
            });
        }
        else {
            // use a sample static input
            rdd = context.parallelizePairs(getSampleInput());
        }
        
        // How to use combineByKey():
        // to use combineByKey(), you need to define 3 basic functions f1, f2, f3:
        // and then you invoke it as: combineByKey(f1, f2, f3)
        
        // function 1: 
        Function<Double, AvgCount> createAcc = (Double x) -> new AvgCount(x, 1);
        
        // function 2
        Function2<AvgCount, Double, AvgCount> addAndCount = (AvgCount a, Double x) -> {
            a.total += x;
            a.num += 1;
            return a;
        };
        
        // function 3
        Function2<AvgCount, AvgCount, AvgCount> combine = (AvgCount a, AvgCount b) -> {
            a.total += b.total;
            a.num += b.num;
            return a;
        };
        
        // now that we have defined 3 functions, we can use combineByKey()
        JavaPairRDD<String, AvgCount> avgCounts = rdd.combineByKey(
                createAcc, 
                addAndCount, 
                combine);
        //
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
        
        // done
        context.close();
    }
}
