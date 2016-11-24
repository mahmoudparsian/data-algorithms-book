package org.dataalgorithms.chap26.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * Gene Aggregation by Average 
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object GeneAggregationByAverage {
  //
  def main(args: Array[String]): Unit = {
    if (args.size < 5) {
      println("Usage: GeneAggregationByAverage <input-path> <sample-reference-type> <filter-type> <filter-value-threshold> <output-path>")
      // <sample-reference-type> can be one of the following:
      //      "r1" = normal sample
      //      "r2" = disease sample
      //      "r3" = paired sample
      //      "r4" = unknown      
      sys.exit(1)
    }
    
    val sparkConf = new SparkConf().setAppName("GeneAggregationByAverage")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val referenceType = args(1)   // {"r1", "r2", "r3", "r4"}
    val filterType = args(2)      // {"up", "down", "abs"}
    val filterValueThreshold = args(3).toDouble
    val output = args(4)

    val broadcastReferenceType = sc.broadcast(referenceType)
    val broadcastFilterType = sc.broadcast(filterType)
    val broadcastFilterValueThreshold = sc.broadcast(filterValueThreshold)

    val records = sc.textFile(input)

    val genes = records.flatMap(record => {
      val tokens = record.split(";")
      val geneIDAndReferenceType = tokens(0)
      val referenceType = geneIDAndReferenceType.split(",")(1)
      val patientIDAndGeneValue = tokens(1)
      if (referenceType == broadcastReferenceType.value) ((geneIDAndReferenceType, patientIDAndGeneValue) :: Nil) else Nil
    })

    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance 
    val genesByID = genes.groupByKey()

    val frequency = genesByID.mapValues(itr => {
      val patients = itr.map(str => {
        val tokens = str.split(",")
        (tokens(0), tokens(1).toDouble)
      }).groupBy(_._1).mapValues(itr => {
        val geneValues = itr.map(_._2)
        (geneValues.sum / geneValues.size)
      })
      val averages = patients.values
      broadcastFilterType.value match {
        case "up"   => averages.filter(_ >= broadcastFilterValueThreshold.value).size
        case "down" => averages.filter(_ <= broadcastFilterValueThreshold.value).size
        case "abs"  => averages.filter(math.abs(_) >= broadcastFilterValueThreshold.value).size
        case _      => 0
      }
    })

    // For debugging
    frequency.foreach(println)

    frequency.saveAsTextFile(output)

    // done
    sc.stop()
  }
}