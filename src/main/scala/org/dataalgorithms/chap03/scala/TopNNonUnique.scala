package org.dataalgorithms.chap03.scala;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.SortedMap

/**
 * Find TopN (N > 0) using mapPartitions().
 * Each partition finds TopN, then we find TopN of all partitions.
 *           
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 * 
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object TopNNonUnique {
  
  def main(args: Array[String]): Unit = {
    val config = new SparkConf
    config.setAppName("TopNNonUnique")
    config.setMaster("local[*]")
    val sc = new SparkContext(config)

    val N = sc.broadcast(2)
    val path = "/path/to/the/folder"

    val input = sc.textFile(path)
    val kv = input.map(line => {
      val tokens = line.split(",")
      (tokens(0), tokens(1).toInt)
    })
    
    val uniqueKeys = kv.reduceByKey(_ + _)
    import Ordering.Implicits._
    val partitions = uniqueKeys.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, String]
      itr.foreach { tuple => {
        sortedMap += tuple.swap
        if (sortedMap.size > N.value) {
          sortedMap = sortedMap.takeRight(N.value)
        }
      }}
      sortedMap.takeRight(N.value).toIterator
    })
  
    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, String].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(N.value)
    resultUsingMapPartition.foreach { case (k, v) => println(s"$k \t ${v.mkString(",")}") }

    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => (a + b)
    val moreConciseApproach = kv.combineByKey(createCombiner, mergeValue, mergeValue)
                   .map(_.swap)
                   .groupByKey()
                   .sortByKey(false).take(N.value)
    //              
    moreConciseApproach.foreach { 
      case (k, v) => println(s"$k \t ${v.mkString(",")}") 
    }

    // done
    sc.stop()
  }
}
