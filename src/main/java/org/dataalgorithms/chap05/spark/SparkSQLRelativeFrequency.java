package org.dataalgorithms.chap05.spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
//
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * This solution implements "Relative Frequency" design pattern using Spark SQL
 *
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
public class SparkSQLRelativeFrequency {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SparkSQLRelativeFrequency <neighbor-window> <input-dir> <output-dir>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkSQLRelativeFrequency");
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLRelativeFrequency")
                .config(sparkConf)
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        int neighborWindow = Integer.parseInt(args[0]);
        String input = args[1];
        String output = args[2];

        final Broadcast<Integer> brodcastWindow = sc.broadcast(neighborWindow);

        /*
         * Schema (word, neighbour, frequency)
         */
        StructType rfSchema = new StructType(new StructField[]{
            new StructField("word", DataTypes.StringType, false, Metadata.empty()),
            new StructField("neighbour", DataTypes.StringType, false, Metadata.empty()),
            new StructField("frequency", DataTypes.IntegerType, false, Metadata.empty())});

        JavaRDD<String> rawData = sc.textFile(input);

        /*
	 * Transform the input to the format: (word, (neighbour, 1))
         */
        JavaRDD<Row> rowRDD = rawData
                .flatMap(new FlatMapFunction<String, Row>() {
                    private static final long serialVersionUID = 5481855142090322683L;

                    @Override
                    public Iterator<Row> call(String line) throws Exception {
                        List<Row> list = new ArrayList<>();
                        String[] tokens = line.split("\\s");
                        for (int i = 0; i < tokens.length; i++) {
                            int start = (i - brodcastWindow.value() < 0) ? 0
                                    : i - brodcastWindow.value();
                            int end = (i + brodcastWindow.value() >= tokens.length) ? tokens.length - 1
                                    : i + brodcastWindow.value();
                            for (int j = start; j <= end; j++) {
                                if (j != i) {
                                    list.add(RowFactory.create(tokens[i], tokens[j], 1));
                                } else {
                                    // do nothing
                                    continue;
                                }
                            }
                        }
                        return list.iterator();
                    }
                });

        Dataset<Row> rfDataset = spark.createDataFrame(rowRDD, rfSchema);

        rfDataset.createOrReplaceTempView("rfTable");

        String query = "SELECT a.word, a.neighbour, (a.feq_total/b.total) rf "
                + "FROM (SELECT word, neighbour, SUM(frequency) feq_total FROM rfTable GROUP BY word, neighbour) a "
                + "INNER JOIN (SELECT word, SUM(frequency) as total FROM rfTable GROUP BY word) b ON a.word = b.word";
        Dataset<Row> sqlResult = spark.sql(query);

        sqlResult.show(); // print first 20 records on the console
        sqlResult.write().parquet(output + "/parquetFormat"); // saves output in compressed Parquet format, recommended for large projects.
        sqlResult.rdd().saveAsTextFile(output + "/textFormat"); // to see output via cat command

        // done
        sc.close();
        spark.stop();

    }
}
