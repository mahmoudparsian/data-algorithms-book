package org.dataalgorithms.chap09.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * FriendRecommendation: recommed friends...
 * 
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
object FriendRecommendation {
  
  def main(args: Array[String]): Unit = {
    //
    if (args.size < 2) {
      println("Usage: FriendRecommendation <input-path> <output-path>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("FriendRecommendation")
    val sc = new SparkContext(sparkConf)

    val input = args(0)
    val output = args(1)

    val records = sc.textFile(input)

    val pairs = records.flatMap(line => {
      val tokens = line.split("\\s")
      val person = tokens(0).toLong
      val friends = tokens(1).split(",").map(_.toLong).toList
      val mapperOutput = friends.map(directFriend => (person, (directFriend, -1.toLong)))

      val result = for {
        fi <- friends
        fj <- friends
        possibleFriend1 = (fj, person)
        possibleFriend2 = (fi, person)
        if (fi != fj)
      } yield {
        (fi, possibleFriend1) :: (fj, possibleFriend2) :: List()
      }
      mapperOutput ::: result.flatten
    })
    
    //
    // note that groupByKey() provides an expensive solution 
    // [you must have enough memory/RAM to hold all values for 
    // a given key -- otherwise you might get OOM error], but
    // combineByKey() and reduceByKey() will give a better 
    // scale-out performance
    //  
    val grouped = pairs.groupByKey()

    val result = grouped.mapValues(values => {
      val mutualFriends = new collection.mutable.HashMap[Long, List[Long]].empty
      values.foreach(t2 => {
        val toUser = t2._1
        val mutualFriend = t2._2
        val alreadyFriend = (mutualFriend == -1)
        if (mutualFriends.contains(toUser)) {
          if (alreadyFriend) {
            mutualFriends.put(toUser, List.empty)
          } else if (mutualFriends.get(toUser).isDefined && mutualFriends.get(toUser).get.size > 0 && !mutualFriends.get(toUser).get.contains(mutualFriend)) {
            val existingList = mutualFriends.get(toUser).get
            mutualFriends.put(toUser, (mutualFriend :: existingList))
          }
        } else {
          if (alreadyFriend) {
            mutualFriends.put(toUser, List.empty)
          } else {
            mutualFriends.put(toUser, List(mutualFriend))
          }
        }
      })
      mutualFriends.filter(!_._2.isEmpty).toMap
    })

    result.saveAsTextFile(output)

    //
    // formatting and printing it to console for debugging purposes...
    // 
    result.foreach(f => {
      val friends = if (f._2.isEmpty) "" else {
        val items = f._2.map(tuple => (tuple._1, "(" + tuple._2.size + ": " + tuple._2.mkString("[", ",", "]") + ")")).map(g => "" + g._1 + " " + g._2)
        items.toList.mkString(",")
      }
      println(s"${f._1}: ${friends}")
    })

    // done
    sc.stop();
  }
}