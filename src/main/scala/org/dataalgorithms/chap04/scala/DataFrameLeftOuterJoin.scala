package org.dataalgorithms.chap04.scala

import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * This approach used DataFrame add in Spark 1.3 and above.
 * DataFrame are quite powerful yet easy to use, hence higly
 * recommended.
 *
 * This program shows two approaches:
 * 1. Using DataFrame API
 * 2. Using SQL query (this is more easy approach).
 *    We used exactly same query as shown in Query 3 page 88.
 *
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */

object DataFrameLeftOuterJoin {

  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      println("Usage: DataFrameLeftOuterJoin <users-data-path> <transactions-data-path> <output-path>")
      sys.exit(1)
    }

    val usersInputFile = args(0)
    val transactionsInputFile = args(1)
    val output = args(2)

    val sparkConf = new SparkConf()

    // Use for Spark 1.6.2 or below
    // val sc = new SparkContext(sparkConf)
    // val spark = new SQLContext(sc) 

    // Use below for Spark 2.0.0
    val spark = SparkSession
      .builder()
      .appName("DataFram LeftOuterJoin")
      .config(sparkConf)
      .getOrCreate()

    // Use below for Spark 2.0.0
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.types._

    // Define user schema
    val userSchema = StructType(Seq(
      StructField("userId", StringType, false),
      StructField("location", StringType, false)))

    // Define transaction schema
    val transactionSchema = StructType(Seq(
      StructField("transactionId", StringType, false),
      StructField("productId", StringType, false),
      StructField("userId", StringType, false),
      StructField("quantity", IntegerType, false),
      StructField("price", DoubleType, false)))

    def userRows(line: String): Row = {
      val tokens = line.split("\t")
      Row(tokens(0), tokens(1))
    }

    def transactionRows(line: String): Row = {
      val tokens = line.split("\t")
      Row(tokens(0), tokens(1), tokens(2), tokens(3).toInt, tokens(4).toDouble)
    }

    val usersRaw = sc.textFile(usersInputFile) // Loading user data
    val userRDDRows = usersRaw.map(userRows(_)) // Converting to RDD[org.apache.spark.sql.Row]
    val users = spark.createDataFrame(userRDDRows, userSchema) // obtaining DataFrame from RDD

    val transactionsRaw = sc.textFile(transactionsInputFile) // Loading transactions data
    val transactionsRDDRows = transactionsRaw.map(transactionRows(_)) // Converting to  RDD[org.apache.spark.sql.Row]
    val transactions = spark.createDataFrame(transactionsRDDRows, transactionSchema) // obtaining DataFrame from RDD

    //
    // Approach 1 using DataFrame API
    // 
    val joined = transactions.join(users, transactions("userId") === users("userId")) // performing join on on userId
    joined.printSchema() //Prints schema on the console

    val product_location = joined.select(joined.col("productId"), joined.col("location")) // Selecting only productId and location
    val product_location_distinct = product_location.distinct // Getting only disting values
    val products = product_location_distinct.groupBy("productId").count()
    products.show() // Print first 20 records on the console
    products.write.save(output + "/approach1") // Saves output in compressed Parquet format, recommended for large projects.
    products.rdd.saveAsTextFile(output + "/approach1_textFormat") // Converts DataFram to RDD[Row] and saves it to in text file. To see output use cat command, e.g. cat output/approach1_textFormat/part-00*
    // Approach 1 ends

    //
    // Approach 2 using plain old SQL query
    // 
    // Use below for Spark 1.6.2 or below
    // users.registerTempTable("users") // Register as table (temporary) so that query can be performed on the table
    // transactions.registerTempTable("transactions") // Register as table (temporary) so that query can be performed on the table
    
    // Use below for Spark 2.0.0
    users.createOrReplaceTempView("users") // Register as table (temporary) so that query can be performed on the table
    transactions.createOrReplaceTempView("transactions") // Register as table (temporary) so that query can be performed on the table

    import spark.sql

    // Using Query 3 given on Page 88
    val sqlResult = sql("SELECT productId, count(distinct location) locCount FROM transactions LEFT OUTER JOIN users ON transactions.userId = users.userId group by productId")
    sqlResult.show() // Print first 20 records on the console
    sqlResult.write.save(output + "/approach2") // Saves output in compressed Parquet format, recommended for large projects.
    sqlResult.rdd.saveAsTextFile(output + "/approach2_textFormat") // Converts DataFram to RDD[Row] and saves it to in text file. To see output use cat command, e.g. cat output/approach2_textFormat/part-00*
    // Approach 2 ends

    // done
    spark.stop()
  }

}
