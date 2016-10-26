package org.dataalgorithms.chap16.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * Count and identify triangles for a given graph
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object CountTriangles {
  //
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("Usage: CountTriangles <input-path> <output-path>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("CountTriangles")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val lines = sc.textFile(input)

    val edges = lines.flatMap(line => {
      val tokens = line.split("\\s+")
      val start = tokens(0).toLong
      val end = tokens(1).toLong
      (start, end) :: (end, start) :: Nil
    })

    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance
    val triads = edges.groupByKey()

    val possibleTriads = triads.flatMap(tuple => {
      val values = tuple._2.toList
      val result = values.map(v => {
        ((tuple._1, v), 0L)
      })
      val sorted = values.sorted
      val combinations = sorted.combinations(2).map { case Seq(a, b) => (a, b) }.toList
      combinations.map((_, tuple._1)) ::: result
    })

    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance
    val triadsGrouped = possibleTriads.groupByKey()

    val trianglesWithDuplicates = triadsGrouped.flatMap(tg => {
      val key = tg._1
      val values = tg._2
      val list = values.filter(_ != 0)
      if (values.exists(_ == 0)) {
        if (list.isEmpty) Nil

        list.map(l => {
          val sortedTriangle = (key._1 :: key._2 :: l :: Nil).sorted
          (sortedTriangle(0), sortedTriangle(1), sortedTriangle(2))
        })
      } else Nil
    })

    val uniqueTriangles = trianglesWithDuplicates distinct

    // For debugging purpose
    uniqueTriangles.foreach(println)

    uniqueTriangles.saveAsTextFile(output)

    // done
    sc.stop()

  }
}