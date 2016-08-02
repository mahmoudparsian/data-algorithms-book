package org.dataalgorithms.chap10.spark;

//STEP-0: import required classes and interfaces
import org.dataalgorithms.util.SparkUtil;
import scala.Tuple2;
import org.dataalgorithms.util.Tuple3;
import org.dataalgorithms.util.Tuple7;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;


/**
 * The MovieRecommendationsWithJoin is a Spark program to implement a basic
 * movie recommendation engine using "join" for a given set of rated movies.
 *
 * @author Mahmoud Parsian
 *
 */
public class MovieRecommendationsWithJoin {
  public static void main(String[] args) throws Exception {

    //STEP-1: handle input parameters
    if (args.length < 1) {
       System.err.println("Usage: MovieRecommendationsWithJoin <users-ratings>");
       System.exit(1);
    }
    String usersRatingsInputFile = args[0];
    System.out.println("usersRatingsInputFile="+ usersRatingsInputFile);
       
    //STEP-2: create a Spark's context object
    JavaSparkContext ctx = SparkUtil.createJavaSparkContext();
					
    //STEP-3: read HDFS file and create the first RDD
    JavaRDD<String> usersRatings = ctx.textFile(usersRatingsInputFile, 1);
  
	//STEP-4: find who has seen this movie
    // <K2,V2> JavaPairRDD<K2,V2> mapToPair(PairFunction<T,K2,V2> f)
    // Return a new RDD by applying a function to all elements of this RDD.
    // PairFunction<T, K2, V2>	
    // T => Tuple2<K2, V2>
    // T = <user> <movie> <rating>
    // K2 = <movie>
    // V2 = Tuple2<user, rating>
    JavaPairRDD<String,Tuple2<String,Integer>> moviesRDD = 
          //                                      T       K       V
          usersRatings.mapToPair(new PairFunction<String, String, Tuple2<String,Integer>>() {
          //                                            T
      @Override
      public Tuple2<String,Tuple2<String,Integer>> call(String s) {
      	String[] record = s.split("\t");
      	String user = record[0];
      	String movie = record[1];
      	Integer rating = new Integer(record[2]);
      	Tuple2<String,Integer> userAndRating = new Tuple2<String,Integer>(user, rating);
        return new Tuple2<String,Tuple2<String,Integer>>(movie, userAndRating);
      }
    });
    
    System.out.println("=== debug1: moviesRDD: K = <movie>, V = Tuple2<user, rating> ===");
    List<
         Tuple2<String,Tuple2<String,Integer>>
        >  debug1 = moviesRDD.collect();
    for (Tuple2<String,Tuple2<String,Integer>> t2 : debug1) {
      System.out.println("debug1 key="+t2._1 + "\t value="+t2._2);
    }

    
	//STEP-5: group moviesRDD by movie
    JavaPairRDD<String, Iterable<Tuple2<String,Integer>>> moviesGrouped =  moviesRDD.groupByKey();  

    System.out.println("=== debug2: moviesGrouped: K = <movie>, V = Iterable<Tuple2<user, rating>> ===");
    List<
         Tuple2<String,Iterable<Tuple2<String,Integer>>>
        >  debug2 = moviesGrouped.collect();
    for (Tuple2<String,Iterable<Tuple2<String,Integer>>> t2 : debug2) {
      System.out.println("debug2 key="+t2._1 + "\t value="+t2._2);
    }

	//STEP-6: find number of raters per movie and then create (K,V) pairs as
	//        K = user
	//        V = Tuple3<movie, rating, numberOfRaters>
	//
    // PairFlatMapFunction<T, K, V>	
    // T => Iterable<Tuple2<K, V>>
    JavaPairRDD<String,Tuple3<String,Integer,Integer>> usersRDD = 
         //                                                  T                                                 K       V 
         moviesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String,Integer>>>, String, Tuple3<String,Integer,Integer>>() {
      @Override
      public Iterator<Tuple2<String,Tuple3<String,Integer,Integer>>> call(Tuple2<String, Iterable<Tuple2<String,Integer>>> s) {
         List<Tuple2<String,Integer>> listOfUsersAndRatings = new ArrayList<Tuple2<String,Integer>>();
     	 // now read inputs and generate desired (K,V) pairs
     	 String movie = s._1;
      	 Iterable<Tuple2<String,Integer>> pairsOfUserAndRating = s._2;
         int numberOfRaters = 0;
       	 for (Tuple2<String,Integer> t2 : pairsOfUserAndRating) {
       		numberOfRaters++;
       		listOfUsersAndRatings.add(t2);
       	 }
       	
       	 // now emit (K, V) pairs
         List<Tuple2<String,Tuple3<String,Integer,Integer>>> results = new ArrayList<Tuple2<String,Tuple3<String,Integer,Integer>>>();
       	 for (Tuple2<String,Integer> t2 : listOfUsersAndRatings) {
       		String user = t2._1;
       		Integer rating = t2._2;
       		Tuple3<String,Integer,Integer> t3 = new Tuple3<String,Integer,Integer>(movie, rating, numberOfRaters);
       		results.add(new Tuple2<String, Tuple3<String,Integer,Integer>>(user, t3));
         }

         return results.iterator();
      }
    });
    
    System.out.println("=== debug3: moviesGrouped: K = user,  V = Tuple3<movie, rating, numberOfRaters>   ===");
    List<
         Tuple2<String,Tuple3<String,Integer,Integer>>
        >  debug3 = usersRDD.collect();
    for (Tuple2<String,Tuple3<String,Integer,Integer>> t2 : debug3) {
      System.out.println("debug3 key="+t2._1 + "\t value="+t2._2);
    }
    
    // usersRDD = <K=user, V=<movie,rating,numberOfRaters>>
    
	// public <W> JavaPairRDD<K,scala.Tuple2<V,W>> join(JavaPairRDD<K,W> other)
	// Return an RDD containing all pairs of elements with matching keys in 
	// this and other. Each pair of elements will be returned as a (k, (v1, v2)) tuple,
	// where (k, v1) is in this and (k, v2) is in other. Performs a hash join across the cluster.    
    JavaPairRDD<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>> 
         joinedRDD = usersRDD.join(usersRDD);
    // joinedRDD = (user, T2((m1,r1,n1), (m2,r2,n2)) )
    List<
         Tuple2<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>>
        >  debug5 = joinedRDD.collect();
    for (Tuple2<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>> t2 : debug5) {
      System.out.println("debug5 key="+t2._1 + "\t value="+t2._2);
    }
    
 	// public JavaPairRDD<K,V> filter(Function<scala.Tuple2<K,V>,Boolean> f)
	// Return a new RDD containing only the elements that satisfy a predicate. 
    JavaPairRDD<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>> 
          //                                      
      filteredRDD = joinedRDD.filter(new Function<
                                                   Tuple2<String,Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>>, 
                                                    Boolean
                                                 >() {
      public Boolean call(Tuple2<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>> s) {
      	 Tuple3<String,Integer,Integer> movie1 = s._2._1;
      	 Tuple3<String,Integer,Integer> movie2 = s._2._2;
      	 String movieName1  = movie1._1;
      	 String movieName2  = movie2._1;
      	 if (movieName1.compareTo(movieName2) < 0) {
      	 	return true;
      	 }
      	 else {
      	 	return false;
      	 }
      }
    });
    
    List<
         Tuple2<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>>
        >  debug55 = filteredRDD.collect();
    for (Tuple2<String, Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>> t2 : debug55) {
      System.out.println("debug55 key="+t2._1 + "\t value="+t2._2);
    }
	
    // now we have: filteredRDD = (user, T2((m1,r1,n1), (m2,r2,n2)) ) where m1 < m2
    // to guarntee that we will not have duplicate movie pais per user
    

    //STEP-8: generate all (movie1, movie2) combinations
    // The goal of this step is to create (K,V) pairs of
    //    K: Tuple2(movie1, movie2)
    //    V: Tuple7(movie1.rating,
    //              movie1.numOfRaters,
    //              movie2.rating,
    //              movie2.numOfRaters,
    //              ratingProduct, 
    //              rating1Squared, = movie1.rating * movie1.rating
    //              rating2Squared  = movie2.rating * movie2.rating
    //             )    
	// PairFunction<T, K, V>	
	// T => Tuple2<K, V>
	// user attribute will be dropped at this phase.
	//          K                      V             
    JavaPairRDD<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> 
          moviePairs = filteredRDD.mapToPair(new PairFunction
               <Tuple2<String,Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>>,       // T
                Tuple2<String,String>,                                          // K
                Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> // V
               >() {
      @Override
      public  Tuple2<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>  
        call(Tuple2<String,Tuple2<Tuple3<String,Integer,Integer>,Tuple3<String,Integer,Integer>>> s) {
            // String user = s._1; // will be dropped
            Tuple3<String,Integer,Integer> movie1 = s._2._1;
            Tuple3<String,Integer,Integer> movie2 = s._2._2;
            Tuple2<String,String> m1m2Key = new Tuple2<String,String>(movie1._1, movie2._1);
	        int ratingProduct = movie1._2 * movie2._2;  // movie1.rating * movie2.rating;
	        int rating1Squared = movie1._2 * movie1._2; // movie1.rating * movie1.rating;
	        int rating2Squared = movie2._2 * movie2._2; // movie2.rating * movie2.rating;
            Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> t7 = 
	           new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(
	                 movie1._2, // movie1.rating,
	                 movie1._3,  // movie1.numberOfRaters,
	                 movie2._2, // movie2.rating,
	                 movie2._3,  // movie2.numberOfRaters,
	                 ratingProduct,
	                 rating1Squared,
	                 rating2Squared); 
	        return new Tuple2<Tuple2<String,String>,Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>(m1m2Key, t7);
       }
    });
    
    // here we perform a union() on usersRDD and transactionsRDD
    JavaPairRDD< Tuple2<String,String>, 
                 Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>
               > corrRDD = moviePairs.groupByKey();
               
    // coor.key = (movie1,movie2)
    //corr.value= (pearson, cosine, jaccard) correlations  
    JavaPairRDD<Tuple2<String,String>, Tuple3<Double,Double,Double>> corr = 
          corrRDD.mapValues(new Function< Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>,   // input
                                          Tuple3<Double,Double,Double>                                                                       // output
                                        >() {
      @Override
      public Tuple3<Double,Double,Double> call(Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s) {
      	 return calculateCorrelations(s);
      }
    });    
    
    System.out.println("=== Movie Correlations ===");
    List<
         Tuple2< Tuple2<String,String>, Tuple3<Double,Double,Double>>
        >  debug6 = corr.collect();
    for (Tuple2<Tuple2<String,String>, Tuple3<Double,Double,Double>> t2 : debug6) {
      System.out.println("debug5 key="+t2._1 + "\t value="+t2._2);
    }
    
    corr.saveAsTextFile("/movies/output");
    System.exit(0);
  }

  static List<Tuple3<String,Integer,Integer>> toList(Iterable<Tuple3<String,Integer,Integer>> iter) {
     List<Tuple3<String,Integer,Integer>> list = new ArrayList<Tuple3<String,Integer,Integer>>();
     for (Tuple3<String,Integer,Integer> t3 : iter) {
        list.add(t3);
     }
     return list;
  }

  static Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>  
     getTuple7(Tuple3<String,Integer,Integer> movie1, 
               Tuple3<String,Integer,Integer> movie2) {
       // Tuple3<String,Integer,Integer> movie1 = (movie, rating, numberOfRaters)
       // Tuple3<String,Integer,Integer> movie2 = (movie, rating, numberOfRaters)
	   // calculate additional information, which will be needed by correlation
	   int ratingProduct = movie1._2 * movie2._2;  // movie1.rating * movie2.rating;
	   int rating1Squared = movie1._2 * movie1._2; // movie1.rating * movie1.rating;
	   int rating2Squared = movie2._2 * movie2._2; // movie2.rating * movie2.rating;

	   return new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(
	           movie1._2, // movie1.rating,
	           movie1._3,  // movie1.numberOfRaters,
	           movie2._2, // movie2.rating,
	           movie2._3,  // movie2.numberOfRaters,
	           ratingProduct,
	           rating1Squared,
	           rating2Squared);               
  }
  
  static Tuple3<Double,Double,Double> calculateCorrelations(
     Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> values) {
     //Iterator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> iter = values.iterator(); 
     
     //int groupSize = value.size(); // length of each vector
     int groupSize = 0; // length of each vector
     int dotProduct = 0; // sum of ratingProd
     int rating1Sum = 0; // sum of rating1
     int rating2Sum = 0; // sum of rating2
     int rating1NormSq = 0; // sum of rating1Squared
     int rating2NormSq = 0; // sum of rating2Squared
     int maxNumOfumRaters1 = 0;  // max of numOfRaters1
     int maxNumOfumRaters2 = 0;  // max of numOfRaters2
     //while (iter.hasNext()) {
     //Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> t7 = iter.next();
     for (Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> t7 : values) {
              //Tuple7(<rating1>:         t7._1 
              //       <numOfRaters1>:    t7._2 
              //       <rating2>:         t7._3 
              //       <numOfRaters2> :   t7._4
              //       <ratingProduct> :  t7._5
              //       <rating1Squared> : t7._6
              //       <rating2Squared>): t7._7 
        groupSize++;    
        dotProduct += t7._5; // ratingProduct;
        rating1Sum += t7._1; // rating1;
        rating2Sum += t7._3; // rating2;
        rating1NormSq += t7._6; // rating1Squared;
        rating2NormSq += t7._7; // rating2Squared;
        int numOfRaters1 = t7._2;
        if (numOfRaters1 > maxNumOfumRaters1) {
           maxNumOfumRaters1 = numOfRaters1;
        }
        int numOfRaters2 = t7._4;
        if (numOfRaters2 > maxNumOfumRaters2) {
           maxNumOfumRaters2 = numOfRaters2;
        }
     } 
     
     double pearson = calculatePearsonCorrelation(
               groupSize, 
               dotProduct, 
               rating1Sum,
               rating2Sum,
               rating1NormSq,
               rating2NormSq); 
               
     double cosine = calculateCosineCorrelation(dotProduct, 
                                                Math.sqrt(rating1NormSq), 
                                                Math.sqrt(rating2NormSq));
     
     double jaccard = calculateJaccardCorrelation(groupSize, 
                                                  maxNumOfumRaters1, 
                                                  maxNumOfumRaters2);
     
     return  new Tuple3<Double,Double,Double>(pearson, cosine, jaccard);         
  }
 
  

  static double calculatePearsonCorrelation(
                   double size, 
                   double dotProduct, 
                   double rating1Sum,
                   double rating2Sum,
                   double rating1NormSq,
                   double rating2NormSq)  {

     double numerator = size * dotProduct - rating1Sum * rating2Sum;
     double denominator = 
        Math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) * 
        Math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum);
     return numerator / denominator;
  }
  
  /**
   * The cosine similarity between two vectors A, B is
   *   dotProduct(A, B) / (norm(A) * norm(B))
   */
  static double calculateCosineCorrelation(double dotProduct,
                                           double rating1Norm,
                                           double rating2Norm) {
    return dotProduct / (rating1Norm * rating2Norm);
  }

  /**
   * The Jaccard Similarity between two sets A, B is
   *   |Intersection(A, B)| / |Union(A, B)|
   */
  static double calculateJaccardCorrelation(double inCommon,
                                            double totalA,
                                            double totalB) {
    double union = totalA + totalB - inCommon;
    return inCommon / union;
  }    
  
}
