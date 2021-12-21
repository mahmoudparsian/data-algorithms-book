package org.dataalgorithms.chap17.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * Kmer counting for a given K and N.
 * K: to find K-mers
 * N: to find top-N
 * 
 * A kmer or k-mer is a short DNA sequence consisting of a fixed 
 * number (K) of bases. The value of k is usually divisible by 4 
 * so that a kmer can fit compactly into a basevector object. 
 * Typical values include 12, 20, 24, 36, and 48; kmers of these 
 * sizes are referred to as 12-mers, 20-mers, and so forth.
 *
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object Kmer {
  //
  def main(args: Array[String]): Unit = {
    //
    if (args.size < 3) {
      println("Usage: Kmer <input-path-as-a-FASTQ-file(s)> <K> <N>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Kmer")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val K = args(1).toInt
    val N = args(2).toInt

    val broadcastK = sc.broadcast(K)
    val broadcastN = sc.broadcast(N)

    val records = sc.textFile(input)

    // remove the records, which are not an actual sequence data
    val filteredRDD = records.filter(line => {
      !( 
         line.startsWith("@") || 
         line.startsWith("+") || 
         line.startsWith(";") ||
         line.startsWith("!") || 
         line.startsWith("~")
       )
    })
  
    val kmers = filteredRDD.flatMap(_.sliding(broadcastK.value, 1).map((_, 1)))

    // find frequencies of kmers
    val kmersGrouped = kmers.reduceByKey(_ + _)

    val partitions = kmersGrouped.mapPartitions(_.toList.sortBy(_._2).takeRight(broadcastN.value).toIterator)

    val allTopN = partitions.sortBy(_._2, false, 1).take(broadcastN.value)

    // print out top-N kmers
    allTopN.foreach(println)

    // done!
    sc.stop()
  }
}