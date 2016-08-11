package org.dataalgorithms.chap05.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * This solution implements "Relative Frequency" design pattern.
 *
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object RelativeFrequency {
  
  def main(args: Array[String]): Unit = {

    if (args.size < 3) {
      println("Usage: RelativeFrequency <neighbor-window> <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("RelativeFrequency")
    val sc = new SparkContext(sparkConf)

    val neighborWindow = args(0).toInt
    val input = args(1)
    val output = args(2)

    val brodcastWindow = sc.broadcast(neighborWindow)

    val rawData = sc.textFile(input)

    /* 
     * Transform the input to the format:
     * (word, (neighbour, 1))
     */
    val pairs = rawData.flatMap(line => {
      val tokens = line.split("\\s")
      for {
        i <- 0 until tokens.length
        start = if (i - brodcastWindow.value < 0) 0 else i - brodcastWindow.value
        end = if (i + brodcastWindow.value >= tokens.length) tokens.length - 1 else i + brodcastWindow.value
        j <- start to end if (j != i)
      } yield (tokens(i), (tokens(j), 1))
    })

    // (word, sum(word))
    val totalByKey = pairs.map(t => (t._1, t._2._2)).reduceByKey(_ + _)

    val grouped = pairs.groupByKey()

    // (word, (neighbour, sum(neighbour)))
    val uniquePairs = grouped.flatMapValues(_.groupBy(_._1).mapValues(_.unzip._2.sum))

    // (word, ((neighbour, sum(neighbour)), sum(word)))
    val joined = uniquePairs join totalByKey

    // ((key, neighbour), sum(neighbour)/sum(word))
    val relativeFrequency = joined.map(t => {
      ((t._1, t._2._1._1), (t._2._1._2.toDouble / t._2._2.toDouble))
    })

    // For saving the output in tab separated format
    // ((key, neighbour), relative_frequency)
    val formatResult_tab_separated = relativeFrequency.map(t => t._1._1 + "\t" + t._1._2 + "\t" + t._2)
    formatResult_tab_separated.saveAsTextFile(output)

    // done
    sc.stop()
  }
}
