[Introduction to MapReduce](./Introduction-to-MapReduce.pdf)
===========================
This is an [introductory and companion chapter](./Introduction-to-MapReduce.pdf) 
for the [Data Algorithms book](http://shop.oreilly.com/product/0636920033950.do) 
on MapReduce programming model. Examples are provided in Spark and Hadoop.

Spark Examples
==============
* Character Count: Basic 
````
org.dataalgorithms.bonus.charcount.spark.basic.CharCount
````

* Character Count: Using InMapper Design Pattern
````
org.dataalgorithms.bonus.charcount.mapreduce.inmapper.CharCountInMapperCombiner
````



Hadoop Examples
===============
* Character Count: Basic MapReduce 
````
org.dataalgorithms.bonus.charcount.mapreduce.basic.CharCountDriver
org.dataalgorithms.bonus.charcount.mapreduce.basic.CharCountMapper
org.dataalgorithms.bonus.charcount.mapreduce.basic.CharCountReducer
````

* Character Count: MapReduce using InMapper Design Pattern
````
org.dataalgorithms.bonus.charcount.mapreduce.inmapper.CharCountInMapperCombinerDriver
org.dataalgorithms.bonus.charcount.mapreduce.inmapper.CharCountInMapperCombinerMapper
org.dataalgorithms.bonus.charcount.mapreduce.inmapper.CharCountInMapperCombinerReducer
````


Suggestions
===========
Please share your comments/suggestions: mahmoud.parsian@yahoo.com


[![Data Algorithms Book](https://github.com/mahmoudparsian/data-algorithms-book/blob/master/misc/data_algorithms_image.jpg)](http://shop.oreilly.com/product/0636920033950.do)