|Numbers | Statistics  |
|--------|-------------|
|   6    |  min = 2    |
|   9    |  max = 9    |
|   7    |  sum = 27   |
|   2    |  mean = 5.4 |
|   3    |  median = 6 |


# Simulating Spark's mapPartitions() in Hadoop/MapReduce

Spark provides an amazing number of "map" transformations:

* map() -> one-to-one mapping
* flatMap() -> one-to-many mapping
* mapPartitions() -> many-to-one mapping

Spark's mapPartitions() has the following signature:

````
<U> JavaRDD<U> mapPartitions(FlatMapFunction<java.util.Iterator<T>,U> f)
Return a new RDD by applying a function to each partition of this RDD.
````


Spark's mapPartitions() transformation takes a partition and generates 
small amount of information and data structures. For example, mapPartitions() 
is ideal for finding minimum and maximum of all given numbers. To use map(),
it will be very expensive, since you just will create two keys: "min" and "max"
and the burden will be on the two reducers: if you have a lot of numbers, this might be 
be a practical as well (since you have to pass a ton of number for the two reducers
identified by "min" and "max" keys).

Hadoop/MapReduce's Mapper offers three functions, which can be used to simulate 
the Spark's mapPartitions() transformation:

````
// called once at the beginning of the task 
setup()

// called once for each key/value pair in the input split
map()

// called once at the end of the task 
cleanup()
````

