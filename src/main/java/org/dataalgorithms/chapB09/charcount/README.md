[Introduction to MapReduce](./Introduction-to-MapReduce.pdf)
===========================
This is an [introductory and companion chapter](./Introduction-to-MapReduce.pdf) 
for the [Data Algorithms book](http://shop.oreilly.com/product/0636920033950.do) 
on MapReduce programming model. Examples are provided in [Spark](./spark) and 
[Hadoop](./mapreduce).

Spark Examples
==============
* Character Count: Basic 
````
org.dataalgorithms.chapB09.charcount.spark.basic.CharCount
````

* Character Count: Using InMapper Design Pattern
````
org.dataalgorithms.chapB09.charcount.mapreduce.inmapper.CharCountInMapperCombiner
````



Hadoop Examples
===============
* Character Count: Basic MapReduce 
````
org.dataalgorithms.chapB09.charcount.mapreduce.basic.CharCountDriver
org.dataalgorithms.chapB09.charcount.mapreduce.basic.CharCountMapper
org.dataalgorithms.chapB09.charcount.mapreduce.basic.CharCountReducer
````

* Character Count: MapReduce using InMapper Design Pattern
````
org.dataalgorithms.chapB09.charcount.mapreduce.inmapper.CharCountInMapperCombinerDriver
org.dataalgorithms.chapB09.charcount.mapreduce.inmapper.CharCountInMapperCombinerMapper
org.dataalgorithms.chapB09.charcount.mapreduce.inmapper.CharCountInMapperCombinerReducer
````


Suggestions
===========
Please share your comments/suggestions: mahmoud.parsian@yahoo.com


[![Data Algorithms Book](https://github.com/mahmoudparsian/data-algorithms-book/blob/master/misc/data_algorithms_image.jpg)](http://shop.oreilly.com/product/0636920033950.do)