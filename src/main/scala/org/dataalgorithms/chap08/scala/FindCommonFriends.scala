package org.dataalgorithms.chap08.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Find Common Friends
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object FindCommonFriends {
  
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("Usage: FindCommonFriends <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("FindCommonFriends")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val records = sc.textFile(input)

    val pairs = records.flatMap(s => {
      val tokens = s.split(",")
      val person = tokens(0).toLong
      val friends = tokens(1).split("\\s+").map(_.toLong).toList
      val result = for {
        i <- 0 until friends.size
        friend = friends(i)
      } yield {
        if (person < friend)
          ((person, friend), friends)
        else
          ((friend, person), friends)
      }
      result
    })

    val grouped = pairs.groupByKey()

    val commonFriends = grouped.mapValues(iter => {
      val friendCount = for {
        list <- iter
        if !list.isEmpty
        friend <- list
      } yield ((friend, 1))
      friendCount.groupBy(_._1).mapValues(_.unzip._2.sum).filter(_._2 > 1).map(_._1)
    })

    // save the result to the file
    commonFriends.saveAsTextFile(output) 

    //Format result for easy viewing
    val formatedResult = commonFriends.map(
      f => s"(${f._1._1}, ${f._1._2})\t${f._2.mkString("[", ", ", "]")}"
    )
    
    formatedResult.foreach(println)

    // done!
    sc.stop()
  }
}