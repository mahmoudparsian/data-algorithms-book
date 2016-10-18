package org.dataalgorithms.chap12.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
 * This is an example of how to use Spark's mllib KMeans algorithm.
 * This example builds a model and then emits the K centroids.
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object ScalaKMeans {
  //
  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      println("Usage: ScalaKMeans <input-path> <k> <max-iterations> [<runs>]")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val k = args(1).toInt
    val iterations = args(2).toInt
    val runs = if (args.length >= 3) args(3).toInt else 1

    val lines = sc.textFile(input)
    
    // build the vector points
    val points = lines.map(line => {
      val tokens = line.split("\\s+")
      Vectors.dense(tokens.map(_.toDouble))
    })
  
    // build model
    val model = KMeans.train(points, k, iterations, runs, KMeans.K_MEANS_PARALLEL)

    println("Cluster centers:")
    model.clusterCenters.foreach(println)

    // compute cost
    val cost = model.computeCost(points)
    println(s"Cost: ${cost}")

    // done!
    sc.stop()
  }
}