# Moving Average in Scala/Spark

## Introduction
Moving Average is a succession of averages derived from successive segments 
(typically of constant size and overlapping) of a series of values.

## Scala Solution 

Moving Average Scala Solution                                  |  Description                                        |
-------------------------------------------------------------- | --------------------------------------------------- | 
````org.dataalgorithms.chap06.scala.MovingAverage````          |  Uses ````repartitionAndSortWithinPartitions()````  |                          | 
````org.dataalgorithms.chap06.scala.MovingAverageInMemory````  |  Uses ````groupByKey()```` and then sorts in memory |                                     |

## References
* [Moving Average -- wikipedia](https://en.wikipedia.org/wiki/Moving_average)
* [Moving Average -- Math World](http://mathworld.wolfram.com/MovingAverage.html)
* [Moving Averages -- Simple and Exponential](http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_averages)
