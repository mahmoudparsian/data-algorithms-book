package org.dataalgorithms.chap05.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row

/**
 * This solution implements Relative Frequency design pattern using Spark SQL.
 *
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object SparkSQLRelativeFrequency {
  
  def main(args: Array[String]): Unit = {

    if (args.size < 3) {
      println("Usage: SparkSQLRelativeFrequency <neighbor-window> <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkSQLRelativeFrequency")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val sc = spark.sparkContext

    val neighborWindow = args(0).toInt
    val input = args(1)
    val output = args(2)

    val brodcastWindow = sc.broadcast(neighborWindow)

    val rawData = sc.textFile(input)

    /*
    * Schema
    * (word, neighbour, frequency)
    */
    val rfSchema = StructType(Seq(
      StructField("word", StringType, false),
      StructField("neighbour", StringType, false),
      StructField("frequency", IntegerType, false)))

    /* 
     * Transform the input to the format:
     * Row(word, neighbour, 1)
     */
    val rowRDD = rawData.flatMap(line => {
      val tokens = line.split("\\s")
      for {
        i <- 0 until tokens.length
        start = if (i - brodcastWindow.value < 0) 0 else i - brodcastWindow.value
        end = if (i + brodcastWindow.value >= tokens.length) tokens.length - 1 else i + brodcastWindow.value
        j <- start to end if (j != i)
      } yield Row(tokens(i), tokens(j), 1)
    })

    val rfDataFrame = spark.createDataFrame(rowRDD, rfSchema)
    rfDataFrame.createOrReplaceTempView("rfTable")

    import spark.sql

    val query = "SELECT a.word, a.neighbour, (a.feq_total/b.total) rf " +
      "FROM (SELECT word, neighbour, SUM(frequency) feq_total FROM rfTable GROUP BY word, neighbour) a " +
      "INNER JOIN (SELECT word, SUM(frequency) as total FROM rfTable GROUP BY word) b ON a.word = b.word"

    val sqlResult = sql(query)
    sqlResult.show() // print first 20 records on the console
    sqlResult.write.save(output + "/parquetFormat") // saves output in compressed Parquet format, recommended for large projects.
    sqlResult.rdd.saveAsTextFile(output + "/textFormat") // to see output via cat command

    // done
    spark.stop()

  }
}
