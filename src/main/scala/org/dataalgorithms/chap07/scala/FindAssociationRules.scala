package org.dataalgorithms.chap07.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//
import scala.collection.mutable.ListBuffer

/**
 * Market Basket Analysis: find association rules
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object FindAssociationRules {
  
  def main(args: Array[String]): Unit = {

    if (args.size < 2) {
      println("Usage: FindAssociationRules <input-path> <output-path>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("market-basket-analysis")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val transactions = sc.textFile(input)

    val patterns = transactions.flatMap(line => {
      val items = line.split(",").toList // Converting to List is required because Spark doesn't partition on Array (as returned by split method)
      (0 to items.size) flatMap items.combinations filter (xs => !xs.isEmpty)
    }).map((_, 1))

    val combined = patterns.reduceByKey(_ + _)

    val subpatterns = combined.flatMap(pattern => {
      val result = ListBuffer.empty[Tuple2[List[String], Tuple2[List[String], Int]]]
      result += ((pattern._1, (Nil, pattern._2)))

      val sublist = for {
        i <- 0 until pattern._1.size
        xs = pattern._1.take(i) ++ pattern._1.drop(i + 1)
        if xs.size > 0
      } yield (xs, (pattern._1, pattern._2))
      result ++= sublist
      result.toList
    })

    val rules = subpatterns.groupByKey()

    val assocRules = rules.map(in => {
      val fromCount = in._2.find(p => p._1 == Nil).get
      val toList = in._2.filter(p => p._1 != Nil).toList
      if (toList.isEmpty) Nil
      else {
        val result =
          for {
            t2 <- toList
            confidence = t2._2.toDouble / fromCount._2.toDouble
            difference = t2._1 diff in._1
          } yield (((in._1, difference, confidence)))
        result
      }
    })

    // Formatting the result just for easy reading.
    val formatResult = assocRules.flatMap(f => {
      f.map(s => (s._1.mkString("[", ",", "]"), s._2.mkString("[", ",", "]"), s._3))
    })
    formatResult.saveAsTextFile(output)

    // done!
    sc.stop()
  }
}