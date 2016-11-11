package org.dataalgorithms.chap24.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * DNA-base counts for FASTA files using Spark's combineByKey()
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object DNABaseCountFASTAWithCombineByKey {
  //
  def main(args: Array[String]): Unit = {
    //
    if (args.size < 2) {
      println("Usage: DNABaseCountFASTAWithCombineByKey <input-dir> <output-dir>")
      sys.exit(1)
    }
    
    val sparkConf = new SparkConf().setAppName("DNABaseCountFASTAWithCombineByKey")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val fastaRDD = sc.textFile(input)

    val dnaSequence = fastaRDD.filter(!_.startsWith(">"))

    val basePair = dnaSequence.flatMap (_.toUpperCase().toCharArray().map((_, 1)))

    val result = basePair.combineByKey(
      (count: Int) => count, 
      (c1: Int, c2: Int) => c1 + c2, 
      (c1: Int, c2: Int) => c1 + c2
    )

    // For debugging purpose
    result.foreach(println)

    result.saveAsTextFile(output)

    // done!
    sc.stop()
  }
}