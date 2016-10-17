package org.dataalgorithms.chap11.scala

import java.text.SimpleDateFormat
//
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Markov algorithm
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object Markov {
  //
  def main(args: Array[String]): Unit = {

    if (args.size < 2) {
      println("Usage: Markov <input-path> <output-path>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Markov")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val transactions = sc.textFile(input)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val customers = transactions.map(line => {
      val tokens = line.split(",")
      (tokens(0), (dateFormat.parse(tokens(2)).getTime.toLong, tokens(3).toDouble)) // transaction-id i.e. tokens(1) ignored
    })

    //
    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance
    //  
    val coustomerGrouped = customers.groupByKey()

    val sortedByDate = coustomerGrouped.mapValues(_.toList.sortBy(_._1))

    val stateSequence = sortedByDate.mapValues(list => {
      val sequence = for {
        i <- 0 until list.size - 1
        (currentDate, currentPurchase) = list(i)
        (nextDate, nextPurchse) = list(i + 1)
      } yield {
        val elapsedTime = (nextDate - currentDate) / 86400000 match {
          case diff if (diff < 30) => "S" //small
          case diff if (diff < 60) => "M" // medium
          case _                   => "L" // large
        }
        val amountRange = (currentPurchase / nextPurchse) match {
          case ratio if (ratio < 0.9) => "L" // significantly less than
          case ratio if (ratio < 1.1) => "E" // more or less same
          case _                      => "G" // significantly greater than
        }
        elapsedTime + amountRange
      }
      sequence
    })
  

    val model = stateSequence.filter(_._2.size >= 2).flatMap(f => {
      val states = f._2

      val statePair = for {
        i <- 0 until states.size - 1
        fromState = states(i)
        toState = states(i + 1)
      } yield {
        ((fromState, toState), 1)
      }
      statePair
    })

    // build Markov model
    val markovModel = model.reduceByKey(_ + _)

    val markovModelFormatted = markovModel.map(f => f._1._1 + "," + f._1._2 + "\t" + f._2)
    markovModelFormatted.foreach(println)

    markovModelFormatted.saveAsTextFile(output)

    // done!
    sc.stop()

  }
}