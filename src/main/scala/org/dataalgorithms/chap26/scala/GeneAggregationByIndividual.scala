package org.dataalgorithms.chap26.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * Gene Aggregation by Individual 
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object GeneAggregationByIndividual {
  //
  def main(args: Array[String]): Unit = {
    if (args.size < 5) {
      println("Usage: GeneAggregationByIndividual <input-path> <sample-reference-type> <filter-type> <filter-value-threshold> <output-path>")
      // <sample-reference-type> can be one of the following:
      //      "r1" = normal sample
      //      "r2" = disease sample
      //      "r3" = paired sample
      //      "r4" = unknown     
      sys.exit(1)
    }
    
    val sparkConf = new SparkConf().setAppName("GeneAggregationByIndividual")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val referenceType = args(1) // {"r1", "r2", "r3", "r4"}
    val filterType = args(2)    // {"up", "down", "abs"}
    val filterValueThreshold = args(3).toDouble
    val output = args(4)

    val broadcastReferenceType = sc.broadcast(referenceType)
    val broadcastFilterType = sc.broadcast(filterType)
    val broadcastFilterValueThreshold = sc.broadcast(filterValueThreshold)

    def checkFilter(value: Double, filterType: String, filterValueThreshold: Double): Boolean = {
      filterType match {
        case "abs" if (math.abs(value) >= filterValueThreshold) => true
        case "up" if (value >= filterValueThreshold) => true
        case "down" if (value <= filterValueThreshold) => true
        case _ => false
      }
    }

    val records = sc.textFile(input)

    val genes = records.flatMap(record => {
      val tokens = record.split(";")
      val geneIDAndReferenceType = tokens(0)
      val gr = geneIDAndReferenceType.split(",")
      val geneID = gr(0)
      val referenceType = gr(1)
      val patientIDAndGeneValue = tokens(1)
      val pg = patientIDAndGeneValue.split(",")
      val patientID = pg(0)
      val geneValue = pg(1)
      if (referenceType == broadcastReferenceType.value && checkFilter(geneValue.toDouble, broadcastFilterType.value, broadcastFilterValueThreshold.value)) ((geneIDAndReferenceType, 1) :: Nil) else Nil
    })

    val counts = genes.reduceByKey(_ + _)

    // For debugging
    counts.foreach(println)

    counts.saveAsTextFile(output)

    // done
    sc.stop()
  }
}