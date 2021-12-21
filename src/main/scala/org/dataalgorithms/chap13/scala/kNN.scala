package org.dataalgorithms.chap13.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * kNN algorithm in scala/spark.
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object kNN {
  //
  def main(args: Array[String]): Unit = {
    //
    if (args.size < 5) {
      println("Usage: kNN <k-knn> <d-dimension> <R-input-dir> <S-input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("kNN")
    val sc = new SparkContext(sparkConf)

    val k = args(0).toInt 
    val d = args(1).toInt
    val inputDatasetR = args(2)
    val inputDatasetS = args(3)
    val output = args(4)

    val broadcastK = sc.broadcast(k);
    val broadcastD = sc.broadcast(d)

    val R = sc.textFile(inputDatasetR)
    val S = sc.textFile(inputDatasetS)

    /**
     * Calculate the distance between 2 points
     * 
     * @param rAsString as r1,r2, ..., rd
     * @param sAsString as s1,s2, ..., sd
     * @param d as dimention
     */
    def calculateDistance(rAsString: String, sAsString: String, d: Int): Double = {
      val r = rAsString.split(",").map(_.toDouble)
      val s = sAsString.split(",").map(_.toDouble)
      if (r.length != d || s.length != d) Double.NaN else {
        math.sqrt((r, s).zipped.take(d).map { case (ri, si) => math.pow((ri - si), 2) }.reduce(_ + _))
      }
    }

    val cart = R cartesian S

    val knnMapped = cart.map(cartRecord => {
      val rRecord = cartRecord._1
      val sRecord = cartRecord._2
      val rTokens = rRecord.split(";")
      val rRecordID = rTokens(0)
      val r = rTokens(1) // r.1, r.2, ..., r.d
      val sTokens = sRecord.split(";")
      val sClassificationID = sTokens(1)
      val s = sTokens(2) // s.1, s.2, ..., s.d
      val distance = calculateDistance(r, s, broadcastD.value)
      (rRecordID, (distance, sClassificationID))
    })

    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance
    val knnGrouped = knnMapped.groupByKey()

    val knnOutput = knnGrouped.mapValues(itr => {
      val nearestK = itr.toList.sortBy(_._1).take(broadcastK.value)
      val majority = nearestK.map(f => (f._2, 1)).groupBy(_._1).mapValues(list => {
        val (stringList, intlist) = list.unzip
        intlist.sum
      })
      majority.maxBy(_._2)._1
    })

    // for debugging purpose
    knnOutput.foreach(println)
    
    // save output
    knnOutput.saveAsTextFile(output)

    // done!
    sc.stop()
  }
}