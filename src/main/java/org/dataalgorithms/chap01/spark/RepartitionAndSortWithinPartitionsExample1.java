package org.dataalgorithms.chap01.spark;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
//
import scala.Tuple2;
//
import org.apache.spark.SparkConf;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Test "secondary sort" using repartitionAndSortWithinPartitions()
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
 * 
 * @author Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class RepartitionAndSortWithinPartitionsExample1 implements Serializable {

    static List<Tuple2<Integer, Integer>> buildSampleList() {
        //
        List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
        //
        pairs.add(new Tuple2<>(0, 10));
        pairs.add(new Tuple2<>(1, 11));
        pairs.add(new Tuple2<>(2, 12));
        //
        pairs.add(new Tuple2<>(3, 13));
        pairs.add(new Tuple2<>(4, 14));
        pairs.add(new Tuple2<>(5, 15));
        //
        pairs.add(new Tuple2<>(6, 16));
        pairs.add(new Tuple2<>(7, 17));
        pairs.add(new Tuple2<>(8, 18));
        //
        return pairs;
    }
    
    static JavaSparkContext getJavaSparkContext(String appName) {
        SparkConf conf = new SparkConf();
        conf.setAppName(appName);
        return new JavaSparkContext(conf);        
    }
    
    public static void main(String[] args) {

        // create a context
        JavaSparkContext context = getJavaSparkContext("SecondarySort");

        // build an RDD
        List<Tuple2<Integer, Integer>> pairs = buildSampleList();
        JavaPairRDD<Integer, Integer> rdd = context.parallelizePairs(pairs);

        /**
         * Create a partitioner, which will create 3 partitions 
         * based on the value of (key % 3)
         */
        Partitioner partitioner = new Partitioner() {
            @Override
            public int numPartitions() {
                return 3;
            }
            @Override
            public int getPartition(Object key) {
                // will return 0, 1, or 2
                return (Integer) key % 3;
            }
        };

        JavaPairRDD<Integer, Integer> repartitioned = rdd.repartitionAndSortWithinPartitions(partitioner);
        List<List<Tuple2<Integer, Integer>>> partitions = repartitioned.glom().collect();
        
        // examine the output
        System.out.println("partition: 0 : " + partitions.get(0));
        System.out.println("partition: 1 : " + partitions.get(1));
        System.out.println("partition: 2 : " + partitions.get(2));

        // done
        context.close();
    }
}
/*

Shell Script:
-------------
cat ./run_secondary_sort.sh
#!/bin/bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_72.jdk/Contents/Home/
echo "JAVA_HOME=$JAVA_HOME"
#
export BOOK_HOME=/Users/mparsian/zmp/github/data-algorithms-book
export SPARK_HOME=/Users/mparsian/spark-2.0.0-bin-hadoop2.6
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
prog=org.dataalgorithms.chap01.spark.RepartitionAndSortWithinPartitionsExample1
$SPARK_HOME/bin/spark-submit  --class $prog --master local $APP_JAR


Output:
-------
partition: 0 : [(0,10), (3,13), (6,16)]
partition: 1 : [(1,11), (4,14), (7,17)]
partition: 2 : [(2,12), (5,15), (8,18)]

*/