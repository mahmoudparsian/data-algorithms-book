package org.dataalgorithms.chap22.scala

import scala.io.Source;
//
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//
import org.apache.commons.math.stat.inference.TTestImpl;

/**
 *
 * Ttest using Apache's TTestImpl
 * 
 * A Ttest is an analysis of two populations means through the 
 * use of statistical examination; a t-test with two samples is 
 * commonly used, testing the difference between the samples when 
 * the variances of two normal distributions are not known.
 *
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object Ttest {
  
  def main(args: Array[String]): Unit = {
    //
    if (args.size < 3) {
      println("Usage: Ttest <input-timetable> <input-dir-bioset> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Ttest")
    val sc = new SparkContext(sparkConf)

    val inputTimeTable = args(0)
    val inputBioset = args(1)
    val output = args(2)

    // Use this if inputTimeTable is the full path of the file 
    // (including filename) on local file system
    val timeTableFile = Source.fromFile(inputTimeTable).getLines
    val timeTable = timeTableFile.map(line => {
      val tokens = line.split("\\s+")
      (tokens(0), tokens(1).toDouble)
    }).toMap

    // ----------------------------------------------------
    // NOTE: Use this if inputTimeTable is the directory 
    // on distributed file system (like HDFS)
    // ----------------------------------------------------
    /*
    val timeTableRDD = sc.textFile(inputTimeTable)
    val timeTable = timeTableRDD.map(line => {
      val tokens = line.split("\\s+")
      (tokens(0), tokens(1).toDouble)
    }).collect().toMap
    */

    val broadcastTimeTable = sc.broadcast(timeTable)

    val biosets = sc.textFile(inputBioset)
    val pairs = biosets.map(line => {
      val tokens = line.split(",")
      (tokens(0), tokens(1))
    })
  
    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance    
    val grouped = pairs.groupByKey()

    def ttestCalculator(groupA: Array[Double], groupB: Array[Double]): Double = {
      val ttest = new TTestImpl()
      if (groupA.isEmpty || groupB.isEmpty)
        0d
      else if ((groupA.length == 1) && (groupB.length == 1))
        Double.NaN
      else if (groupA.length == 1)
        ttest.tTest(groupA(0), groupB)
      else if (groupB.length == 1)
        ttest.tTest(groupB(0), groupA)
      else
        ttest.tTest(groupA, groupB)
    }

    val ttest = grouped.mapValues(itr => {
      val geneBiosets = itr.toSet
      val timetable = broadcastTimeTable.value
      val (exist, notexist) = timetable.partition(tuple => geneBiosets.contains(tuple._1))
      ttestCalculator(exist.values.toArray, notexist.values.toArray)
    })

    // For debugging purpose
    ttest.foreach(println)

    // Saving the output
    ttest.saveAsTextFile(output)

    // done!
    sc.stop()
  }
}