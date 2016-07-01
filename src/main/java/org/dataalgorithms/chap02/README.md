![Secondary Sorting](./secondary_sorting.png)


Chapter 02: Secondary Sorting
==========
The purpose of this chapter is to have a sorted values 
for reducer keys.  For example, if our input is like 
time series (sample is given below), the the values for 
each key is sorted. 

Package 
=======
org.dataalgorithms.chap02.mapreduce

Classes
=======

Class Name                   | Description                                |
---------------------------- | ------------------------------------------ | 
CompositeKey                 | Defines a composite key                    | 
CompositeKeyComparator       | Defines how a CompositeKey will be sorted  | 
NaturalKeyGroupingComparator | Defines how a NaturalKey is sorted         | 
NaturalKeyPartitioner        | Defines how NaturalKey(s) are partitio     | 
NaturalValue                 | Defines a NaturalValue                     | 
SecondarySortDriver          | Driver to submit a MapReduce job           | 
SecondarySortMapper          | Defines map()                              | 
SecondarySortReducer         | Defines reduce()                           | 

