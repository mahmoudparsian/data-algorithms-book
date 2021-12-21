package org.dataalgorithms.chap24.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * DNA-base counts for FASTA files using Spark's mapPartitions()
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object DNABaseCountFASTAWithMapPartitions {
  //
  def main(args: Array[String]): Unit = {
    //
    if (args.size < 2) {
      println("Usage: DNABaseCountFASTAWithMapPartitions  <input-dir> <output-dir>")
      sys.exit(1)
    }
    
    val sparkConf = new SparkConf().setAppName("DNABaseCountFASTAWithMapPartitions")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val fastaRDD = sc.textFile(input)

    val partitions = fastaRDD.mapPartitions(itr => {
      val mutableMap = collection.mutable.Map.empty[Char, Long]
      itr.filter(!_.startsWith(">")).foreach(_.toUpperCase().toCharArray().foreach(base => {
        val count = mutableMap.getOrElse(base, 0L)
        mutableMap.put(base, count + 1L)
      }))
      mutableMap.toIterator
    })

    val collectPartition = partitions.collect()

    val result = collectPartition.groupBy(_._1).mapValues(_.unzip._2.sum)

    // debug output
    result.foreach(println)  

    // save result in output
    
    // done
    sc.stop()
  }
}