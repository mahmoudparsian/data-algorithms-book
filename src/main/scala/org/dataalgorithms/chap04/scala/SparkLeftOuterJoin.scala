package org.dataalgorithms.chap04.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * This approach uses 'leftOuterJoin' function available on PairRDD.
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */

object SparkLeftOuterJoin {
  
  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      println("Usage: SparkLeftOuterJoin <users> <transactions> <output>")
      sys.exit(1)
    }
    
    val sparkConf = new SparkConf().setAppName("SparkLeftOuterJoin")
    val sc = new SparkContext(sparkConf)

    val usersInputFile = args(0)
    val transactionsInputFile = args(1)
    val output = args(2)

    val usersRaw = sc.textFile(usersInputFile)
    val transactionsRaw = sc.textFile(transactionsInputFile)

    val users = usersRaw.map(line => {
      val tokens = line.split("\t")
      (tokens(0), tokens(1))
    })

    val transactions = transactionsRaw.map(line => {
      val tokens = line.split("\t")
      (tokens(2), tokens(1))
    })

    val joined =  transactions leftOuterJoin users
    // getOrElse is a method availbale on Option which either returns value 
    // (if present) or returns passed value (unknown in this case).
    val productLocations = joined.values.map(f => (f._1, f._2.getOrElse("unknown"))) 
  
    val productByLocations = productLocations.groupByKey()
    
    val productWithUniqueLocations = productByLocations.mapValues(_.toSet) // Converting toSet removes duplicates.
    
    val result = productWithUniqueLocations.map(t => (t._1, t._2.size)) // Return (product, location count) tuple.
    
    result.saveAsTextFile(output) // Saves output to the file.

    // done
    sc.stop()
  }
}
