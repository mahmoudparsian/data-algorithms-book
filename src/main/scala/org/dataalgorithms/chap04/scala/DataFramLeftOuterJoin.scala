package org.dataalgorithms.chap04.scala

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
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

object DataFramLeftOuterJoin {
  
  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      println("Usage: DataFramLeftOuterJoin <users> <transactions> <output>")
      sys.exit(1)
    }
    
    val usersInputFile = args(0)
    val transactionsInputFile = args(1)
    val output = args(2)
    
    val sparkSession = SparkSession.builder.appName("my-spark-app").getOrCreate()  
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    import sqlContext.implicits._
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
      Row(tokens(0), tokens(1), tokens(2), tokens(3), tokens(4))
    }
    
    val usersRaw = sparkContext.textFile(usersInputFile) // Loading user data
    val userRDDRows = usersRaw.map(userRows(_)) // Converting to RDD[org.apache.spark.sql.Row]
    val users = sqlContext.createDataFrame(userRDDRows, userSchema) // obtaining DataFrame from RDD

    val transactionsRaw = sparkContext.textFile(transactionsInputFile) // Loading transactions data
    val transactionsRDDRows = transactionsRaw.map(transactionRows(_)) // Converting to  RDD[org.apache.spark.sql.Row]
    val transactions = sqlContext.createDataFrame(transactionsRDDRows, transactionSchema) // obtaining DataFrame from RDD

    //
    // Approach 1 using DataFrame API
    // 
    val joined = transactions.join(users, transactions("userId") === users("userId")) // performing join on on userId
    joined.printSchema() //Prints schema on the console

    val product_location = joined.select(joined.col("productId"), joined.col("location")) // Selecting only productId and location
    val product_location_distinct = product_location.distinct // Getting only disting values
    val products = product_location_distinct.groupBy("productId").count()
    products.show() // Print first 20 records on the console
    products.write.save(output + "/approach1") // Saves output in Parquet format
    // Approach 1 ends
    
    //
    // Approach 2 using plain old SQL query
    // 
    users.createOrReplaceTempView("users") // Register as table (temporary) so that query can be performed on the table
    transactions.createOrReplaceTempView("transactions") // Register as table (temporary) so that query can be performed on the table

    import sqlContext.sql
    
    // Using Query 3 given on Page 88
    val sqlResult = sql("SELECT productId, count(distinct location) FROM transactions LEFT OUTER JOIN users ON transactions.userId = users.userId group by productId")  
    sqlResult.show() // Print first 20 records on the console
    sqlResult.write.save(output + "/approach2") // Saves output in Parquet format
    // Approach 2 ends

    sqlContext.clearCache()
    
    // done
    sparkContext.stop()
  }

}
