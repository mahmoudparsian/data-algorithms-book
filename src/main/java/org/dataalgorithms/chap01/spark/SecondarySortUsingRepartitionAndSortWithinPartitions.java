package org.dataalgorithms.chap01.spark;

import java.io.Serializable;
//
import scala.Tuple2;
//
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;


/**
 * Spark/Scala solution to secondary sort using repartitionAndSortWithinPartitions()
 * defined as:
 * 
 *      public JavaPairRDD<K,V> repartitionAndSortWithinPartitions(
 *                                     org.apache.spark.Partitioner partitioner,
 *                                     java.util.Comparator<K> comp)
 * 
 *      Description: Repartition the RDD according to the given partitioner and, 
 *                   within each resulting partition, sort records by their keys.
 *                   This is more efficient than calling repartition and then sorting 
 *                   within each partition because it can push the sorting down into 
 *                   the shuffle machinery.
 * 
 *                   Partitioner: an object that defines how the elements in a key-value 
 *                   pair RDD are partitioned by key. Maps each key to a partition ID, 
 *                   from 0 to numPartitions - 1.
 *
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 * 
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class SecondarySortUsingRepartitionAndSortWithinPartitions implements Serializable {

    private static final long serialVersionUID = -4936134638714686170L;

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage <number-of-partitions> <input-dir> <output-dir>");
            System.exit(1);
        }
        //
        int partitions = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];

        SparkConf conf = new SparkConf();
        conf.setAppName("SecondarySort");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath);

        JavaPairRDD<Tuple2<String, Integer>, Integer> valueToKey = 
                input.mapToPair(new PairFunction<String, Tuple2<String, Integer>, Integer>() {

            private static final long serialVersionUID = -8300896341249252441L;

            @Override
            public Tuple2<Tuple2<String, Integer>, Integer> call(String t) throws Exception {
                String[] array = t.split(",");
                Integer value = Integer.parseInt(array[3]);
                Tuple2<String, Integer> key = new Tuple2<String, Integer>(array[0] + "-" + array[1], value);
                return new Tuple2<Tuple2<String, Integer>, Integer>(key, value);
            }
        });

        //TupleComparatorDescending tupleComparatorDescending = new TupleComparatorDescending();
        JavaPairRDD<Tuple2<String, Integer>, Integer> sorted
                = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions), TupleComparatorDescending.INSTANCE);

        JavaPairRDD<String, Integer> result = 
                sorted.mapToPair(new PairFunction<Tuple2<Tuple2<String, Integer>, Integer>, String, Integer>() {

            private static final long serialVersionUID = 7009282886891421220L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Tuple2<String, Integer>, Integer> t) throws Exception {
                return new Tuple2<String, Integer>(t._1._1, t._2);
            }
        });
        //
        result.saveAsTextFile(outputPath);

        sc.close();
    }
}
