[![Rank Product](./rank-product.png)]()

Rank Product
============
The [Rank Product](./RankProduct_chapter.pdf) is defined and two scalable 
Spark algorithms provided:
* ````org.dataalgorithms.chapB04.rankproduct.spark.SparkRankProductUsingGroupByKey````
* ````org.dataalgorithms.chapB04.rankproduct.spark.SparkRankProductUsingCombineByKey````

Note that the solution using ````combineByKey()```` is more efficient than the 
solution using ````groupByKey()````.

The key for using ````combineByKey()```` is to define a basic efficient serializable 
data structure ````C```` and then provide 3 basic functions (````createCombiner()````, 
````mergeValue()````, and ````mergeCombiners()````).

````
public <C> JavaPairRDD<K,C> combineByKey(Function<V,C> createCombiner,
                                         Function2<C,V,C> mergeValue,
                                         Function2<C,C,C> mergeCombiners)

````

* ````createCombiner```` accepts a single input value ````V```` and creates an instance of ````C````
* ````mergeValue()```` accepts two input parameters: ````V```` and ````C```` and then 
   create a new instance of ````C````
* ````mergeCombiners()```` accepts two input parameters: ````C1```` (of type ````C````) and 
  ````C2```` (of type ````C````) and then create a new instance of ````C```` (by merging 
  ````C1```` and ````C2````)


O'Reilly Webinar Slides
=======================
* [Rank Product Slides](./RankProduct_slides.pdf) 
* [Apache Spark Solution for Rank Product](http://www.oreilly.com/pub/e/3507)

Suggestions
===========
Please share your comments/suggestions: mahmoud.parsian@yahoo.com


[![Data Algorithms Book](https://github.com/mahmoudparsian/data-algorithms-book/blob/master/misc/data_algorithms_image.jpg)](http://shop.oreilly.com/product/0636920033950.do)
