package org.dataalgorithms.chap23.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 *
 * Correlate "all-samples" vs. "all-samples".
 * Here we use Apache's PearsonsCorrelation, 
 * but you may replace it with your desired 
 * correlation algorithm.
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object AllVersusAllCorrelation {

  def main(args: Array[String]): Unit = {
    //
    if (args.size < 3) {
      println("Usage: AllVersusAllCorrelation <input-sample-reference> <input-dir-bioset> <output-dir>")
      //
      // <input-sample-reference> can be one of the following:
      //      "r1" = normal sample
      //      "r2" = disease sample
      //      "r3" = paired sample
      //      "r4" = unknown
      //      
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("AllVersusAllCorrelation")
    val sc = new SparkContext(sparkConf)

    val inputReference = args(0) // {"r1", "r2", "r3", "r4"}
    val inputBioset = args(1)
    val output = args(2)

    val ref = sc.broadcast(inputReference)
    val biosets = sc.textFile(inputBioset)

    // keep the sample matching with the <input-sample-reference>
    val filtered = biosets.filter(_.split(",")(1).equals(ref.value))

    val pairs = filtered.map(record => {
      val tokens = record.split(",")
      (tokens(0), (tokens(2), tokens(3).toDouble))
    })

    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance 
    val grouped = pairs.groupByKey()

    val cart = grouped.cartesian(grouped)

    // since "correlation of (A, B)" is the same as 
    // "correlation of (B, A)", we just only keep (A, B) 
    // before further computation and drop (B, A) 
    val filtered2 = cart.filter(pair => pair._1._1 < pair._2._1)

    val finalresult = filtered2.map(t => {
      val g1 = t._1
      val g2 = t._2
      val g1Map = g1._2.toMap.groupBy(_._1).mapValues(itr => (itr.values.sum, itr.values.size))
      val g2Map = g2._2.toMap.groupBy(_._1).mapValues(itr => (itr.values.sum, itr.values.size))
      val xy = for {
        g1Entry <- g1Map
        g1PatientID = g1Entry._1
        g2MD = g2Map.get(g1PatientID)
        if (g2MD.isDefined)
      } yield ((g1Entry._2._1 / g1Entry._2._2, g2MD.get._1 / g2MD.get._2))

      val (x, y) = xy.unzip
      val k = (g1._1, g2._1)
      if (x.size < 3) (k, (Double.NaN, Double.NaN))
      else {
        import org.apache.commons.math3.distribution.TDistribution
        import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
        val PC = new PearsonsCorrelation()
        val correlation = PC.correlation(x.toArray, y.toArray)
        val t = math.abs(correlation * math.sqrt((x.size - 2.0) / (1.0 - (correlation * correlation))))
        val tdist = new TDistribution(x.size - 2)
        val pvalue = 2 * (1.0 - tdist.cumulativeProbability(t))
        (k, (correlation, pvalue))
      }
    })

    // For deugging purpose
    finalresult.foreach(println)

    // save final output
    finalresult.saveAsTextFile(output)

    // done!
    sc.stop()
  }

}