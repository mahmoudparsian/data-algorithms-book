package org.dataalgorithms.chap14.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Use a Naive Bayes Classifier (built by NaiveBayesClassifierBuilder)
 * to classify new data
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object NaiveBayesClassifier {

  def main(args: Array[String]): Unit = {
    //
    if (args.size < 4) {
      println("Usage: NaiveBayesClassifier <input-query-data> <input-pt> <input-classes> <output-path>")
      // <input-query-data> is a new data that we want to classify by using/applying the built Naive Bayes Classifier
      // <input-pt> is a probability table built by the NaiveBayesClassifierBuilder class
      // <input-classes> are the classes built by the NaiveBayesClassifierBuilder class
      // <output-path> is the output of new data classified by the built Naive Bayes Classifier
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("NaiveBayesClassifier")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val nbProbabilityTablePath = args(1)
    val classesPath = args(2)
    val output = args(3)

    val newdata = sc.textFile(input)
    val classifierRDD = sc.objectFile[Tuple2[Tuple2[String, String], Double]](nbProbabilityTablePath)

    val classifier = classifierRDD.collectAsMap()
    val broadcastClassifier = sc.broadcast(classifier)
    val classesRDD = sc.textFile(classesPath)
    val broadcastClasses = sc.broadcast(classesRDD.collect())

    val classified = newdata.map(rec => {
      val classifier = broadcastClassifier.value
      val classes = broadcastClasses.value
      val attributes = rec.split(",")

      val class_score = classes.map(aClass => {
        val posterior = classifier.getOrElse(("CLASS", aClass), 1d)
        val probabilities = attributes.map(attribute => {
          classifier.getOrElse((attribute, aClass), 0d)
        })
        (aClass, probabilities.product * posterior)
      })
      val maxClass = class_score.maxBy(_._2)
      (rec, maxClass._1)
    })

    // For debugging purpose
    classified.foreach(println)

    // saved the classified data
    classified.saveAsTextFile(output)

    // done!
    sc.stop()
  }
}