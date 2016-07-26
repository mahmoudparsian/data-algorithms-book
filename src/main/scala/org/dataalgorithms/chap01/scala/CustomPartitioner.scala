package org.dataalgorithms.chap01.scala

import org.apache.spark.Partitioner


/**
 * A custom partitioner
 * 
 * org.apache.spark.Partitioner:
 *           An abstract class that defines how the elements in a  
 *           key-value pair RDD are partitioned by key. Maps each 
 *           key to a partition ID, from 0 to numPartitions - 1.
 *           
 *
 * @author Gaurav Bhardwaj (gauravbhardwajemail@gmail.com)
 * 
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 */
class CustomPartitioner(partitions: Int) extends Partitioner {
  
  require(partitions > 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case (k: String, v: Int) => math.abs(k.hashCode % numPartitions)
    case null                => 0
    case _                   => math.abs(key.hashCode % numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: CustomPartitioner => h.numPartitions == numPartitions
    case _                    => false
  }

  override def hashCode: Int = numPartitions
}
