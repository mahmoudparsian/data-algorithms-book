# Moving Average in Scala/Spark

Moving Average is a succession of averages derived from successive segments 
(typically of constant size and overlapping) of a series of values.


Moving Average Solution                                        |  Description                                        |
-------------------------------------------------------------- | --------------------------------------------------- | 
````org.dataalgorithms.chap06.scala.MovingAverage````          |  Uses ````repartitionAndSortWithinPartitions()````  |                          | 
````org.dataalgorithms.chap06.scala.MovingAverageInMemory````  |  Uses ````groupByKey()```` and then sorts in memory |                                     |
