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
object TopN {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf
    config.setAppName("TopN")
    config.setMaster("local[*]")
    val sc = new SparkContext(config)

    val N = sc.broadcast(10)
    val path = "/path/to/the/folder"

    val input = sc.textFile(path)
    val pair = input.map(line => {
      val tokens = line.split(",")
      (tokens(2).toInt, tokens)
    })

    import Ordering.Implicits._
    val partitions = pair.mapPartitions(itr => {
      var sortedMap = SortedMap.empty[Int, Array[String]]
      itr.foreach { tuple =>
        {
          sortedMap += tuple
          if (sortedMap.size > N.value) {
            sortedMap = sortedMap.takeRight(N.value)
          }
        }
      }
      sortedMap.takeRight(N.value).toIterator
    })
  
    val alltop10 = partitions.collect()
    val finaltop10 = SortedMap.empty[Int, Array[String]].++:(alltop10)
    val resultUsingMapPartition = finaltop10.takeRight(N.value)
    resultUsingMapPartition.foreach { 
      case (k, v) => println(s"$k \t ${v.asInstanceOf[Array[String]].mkString(",")}") 
    }

    val moreConciseApproach = pair.groupByKey().sortByKey(false).take(N.value)
    moreConciseApproach.foreach { case (k, v) => println(s"$k \t ${v.flatten.mkString(",")}") }

    // done
    sc.stop()
  }
}
