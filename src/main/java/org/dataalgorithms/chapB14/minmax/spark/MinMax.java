package org.dataalgorithms.chapB14.minmax.spark;

import scala.Tuple2;
//
import java.util.List;
import java.util.Iterator;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Description:
 *
 * MinMax: Find minimum and maximum of a set of numbers using mapPartitions()
 *
 * @author Mahmoud Parsian
 *
 */
public class MinMax {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MinMax <input> <output>");
            System.exit(1);
        }

        // handle input parameters
        final String inputPath = args[0];
        final String outputPath = args[1];

        SparkSession spark = SparkSession.builder().appName("minmax").getOrCreate();

        JavaRDD<String> numbers = spark.read().textFile(inputPath).javaRDD();

        // JavaRDD<U> mapPartitions(FlatMapFunction<java.util.Iterator<T>,U> f)
        JavaRDD<Tuple2<Integer, Integer>> partitions = numbers.mapPartitions(
                new FlatMapFunction<Iterator<String>, Tuple2<Integer, Integer>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> iter) {
                int min = 0; // 0 is never used
                int max = 0; // 0 is never used
                boolean firstTime = true;
                while (iter.hasNext()) {
                    int number = Integer.parseInt(iter.next());
                    if (firstTime) {
                        min = number;
                        max = number;
                        firstTime = false;
                    } else {
                        if (number < min) {
                            min = number;
                        }
                        if (number > max) {
                            max = number;
                        }
                    }
                }
                return Util.toIterator(min, max);
            }
        });

        // find a final frequencies table: aggregate values for the same character key
        List<Tuple2<Integer, Integer>> minmaxList = partitions.collect();
        Tuple2<Integer, Integer> minmax =  Util.findMinMax(minmaxList);

        System.out.println("min=" + minmax._1);
        System.out.println("max=" + minmax._2);

        // close the session!  
        spark.close();

        System.exit(0);
    }
}
