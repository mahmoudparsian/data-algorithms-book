package org.dataalgorithms.chap05.spark;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
//
import scala.Tuple2;
//
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * This solution implements "Relative Frequency" design pattern.
 *
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class RelativeFrequency {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: RelativeFrequencyJava <neighbor-window> <input-dir> <output-dir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("RelativeFrequency");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        int neighborWindow = Integer.parseInt(args[0]);
        String input = args[1];
        String output = args[2];

        final Broadcast<Integer> brodcastWindow = sc.broadcast(neighborWindow);

        JavaRDD<String> rawData = sc.textFile(input);

        /*
         * Transform the input to the format: (word, (neighbour, 1))
         */
        JavaPairRDD<String, Tuple2<String, Integer>> pairs = rawData.flatMapToPair(
                new PairFlatMapFunction<String, String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -6098905144106374491L;

            @Override
            public java.util.Iterator<scala.Tuple2<String, scala.Tuple2<String, Integer>>> call(String line) throws Exception {
                List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<Tuple2<String, Tuple2<String, Integer>>>();
                String[] tokens = line.split("\\s");
                for (int i = 0; i < tokens.length; i++) {
                    int start = (i - brodcastWindow.value() < 0) ? 0 : i - brodcastWindow.value();
                    int end = (i + brodcastWindow.value() >= tokens.length) ? tokens.length - 1 : i + brodcastWindow.value();
                    for (int j = start; j <= end; j++) {
                        if (j != i) {
                            list.add(new Tuple2<String, Tuple2<String, Integer>>(tokens[i], new Tuple2<String, Integer>(tokens[j], 1)));
                        } else {
                            // do nothing
                            continue;
                        }
                    }
                }
                return list.iterator();
            }
        }
        );

        // (word, sum(word))
        JavaPairRDD<String, Integer> totalByKey = pairs.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Integer>() {
            private static final long serialVersionUID = -213550053743494205L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, Integer>> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._1, tuple._2._2);
            }
        }).reduceByKey(
                        new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -2380022035302195793L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return (v1 + v2);
                    }
                });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> grouped = pairs.groupByKey();

        // (word, (neighbour, sum(neighbour)))
        JavaPairRDD<String, Tuple2<String, Integer>> uniquePairs = grouped.flatMapValues(
                new FlatMapFunction<Iterable<Tuple2<String, Integer>>, 
                                    Tuple2<String, Integer>
                                   >() {
            // new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
            //private static final long serialVersionUID = 5790208031487657081L;

            @Override
            public Iterator<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> values) 
                    throws Exception {
                //
                Map<String, Integer> map = new HashMap<>();
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                Iterator<Tuple2<String, Integer>> iterator = values.iterator();
                while (iterator.hasNext()) {
                    Tuple2<String, Integer> value = iterator.next();
                    int total = value._2;
                    if (map.containsKey(value._1)) {
                        total += map.get(value._1);
                    }
                    map.put(value._1, total);
                }
                for (Map.Entry<String, Integer> kv : map.entrySet()) {
                    list.add(new Tuple2<String, Integer>(kv.getKey(), kv.getValue()));
                }
                return list.iterator();
            }
        });

        // (word, ((neighbour, sum(neighbour)), sum(word)))
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> joined = uniquePairs.join(totalByKey);

        // ((key, neighbour), sum(neighbour)/sum(word))
        JavaPairRDD<Tuple2<String, String>, Double> relativeFrequency = joined.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>>, Tuple2<String, String>, Double>() {
            private static final long serialVersionUID = 3870784537024717320L;

            @Override
            public Tuple2<Tuple2<String, String>, Double> call(Tuple2<String, Tuple2<Tuple2<String, Integer>, Integer>> tuple) throws Exception {
                return new Tuple2<Tuple2<String, String>, Double>(new Tuple2<String, String>(tuple._1, tuple._2._1._1), ((double) tuple._2._1._2 / tuple._2._2));
            }
        });

        // For saving the output in tab separated format
        // ((key, neighbour), relative_frequency)
        JavaRDD<String> formatResult_tab_separated = relativeFrequency.map(
                new Function<Tuple2<Tuple2<String, String>, Double>, String>() {
            private static final long serialVersionUID = 7312542139027147922L;

            @Override
            public String call(Tuple2<Tuple2<String, String>, Double> tuple) throws Exception {
                return tuple._1._1 + "\t" + tuple._1._2 + "\t" + tuple._2;
            }
        });

        // save output
        formatResult_tab_separated.saveAsTextFile(output);

        // done
        sc.close();

    }
}
