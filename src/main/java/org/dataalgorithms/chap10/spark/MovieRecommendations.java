package org.dataalgorithms.chap10.spark;

//STEP-0: import required classes and interfaces
import org.dataalgorithms.util.SparkUtil;
import org.dataalgorithms.util.Tuple3;
import org.dataalgorithms.util.Tuple7;
import org.dataalgorithms.util.Combination;

import scala.Tuple2;
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
 * The MovieRecommendations is a Spark program to implement a basic
 * movie recommendation engine for a given set of rated movies.
 *
 * @author Mahmoud Parsian
 *
 */
public class MovieRecommendations {
  public static void main(String[] args) throws Exception {

    //STEP-1: handle input parameters
    if (args.length < 1) {
      System.err.println("Usage: MovieRecommendations <users-ratings>");
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
          //                               T       K       V
          usersRatings.mapToPair(new PairFunction<String, String, Tuple2<String,Integer>>() {
          //                                            T
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
    
    
    
    //STEP-7: all movie-and-ratings grouped by user
    JavaPairRDD<String, Iterable<Tuple3<String,Integer,Integer>>> groupedbyUser =  usersRDD.groupByKey();  

    System.out.println("=== debug4: moviesGrouped: K = user,  V = Iterable<Tuple3<movie, rating, numberOfRaters>>   ===");
    List<
         Tuple2<String,Iterable<Tuple3<String,Integer,Integer>>>
        >  debug4 = groupedbyUser.collect();
    for (Tuple2<String,Iterable<Tuple3<String,Integer,Integer>>> t2 : debug4) {
      System.out.println("debug4 key="+t2._1 + "\t value="+t2._2);
    }


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
	//          K                      V             
    JavaPairRDD<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> 
          //                                T       K       V
          moviePairs = groupedbyUser.flatMapToPair(new PairFlatMapFunction
               <Tuple2<String, Iterable<Tuple3<String,Integer,Integer>>>,       // T
                Tuple2<String,String>,                                          // K
                Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> // V
               >() {
      @Override
      public Iterator< 
                      Tuple2< 
                              Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>
                            >
                     >  
        call(Tuple2< String, 
                     Iterable<Tuple3<String,Integer,Integer>>
                   > s) {
        String user = s._1;
        Iterable<Tuple3<String,Integer,Integer>> movies = s._2;
      	// each Tuple3(movie, rating, numberOfRaters)
      	// next, we generate all (movie1, movie2) combinations from "movies"
      	List<Tuple3<String,Integer,Integer>> listOfMovies = toList(movies);
        List<List<Tuple3<String,Integer,Integer>>> comb2 = 
           Combination.findSortedCombinations(listOfMovies, 2); 
        List<Tuple2<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>> results =
        new ArrayList<Tuple2<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>>();
        for (List<Tuple3<String,Integer,Integer>> twoMovies : comb2) {
           Tuple3<String,Integer,Integer> movie1 = twoMovies.get(0);
           Tuple3<String,Integer,Integer> movie2 = twoMovies.get(1);
           //Tuple2<String,String> k3 = new Tuple2<String,String>(movie1._1, movie2._1);
           Tuple2<String,String> k3 = new Tuple2<String,String>(movie1._1, movie2._1);
           Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> v3 = getTuple7(movie1, movie2);
           Tuple2<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> k3v3 =
           new Tuple2<Tuple2<String,String>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>(k3, v3);
           results.add(k3v3);
        }     	
        return results.iterator();
      }
    });
    
    // here we perform a union() on usersRDD and transactionsRDD
    JavaPairRDD< Tuple2<String,String>, 
                 Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>
               > corrRDD = moviePairs.groupByKey();
               
       
    JavaPairRDD<Tuple2<String,String>, Double> corr = 
          corrRDD.mapValues(new Function< Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>,   // input
                                          Double                                                                       // output
                                        >() {
      public Double call(Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> s) {
      	 return calculateCorr(s);
      }
    });    
    
    System.out.println("=== Movie Correlations ===");
    List<
         Tuple2< Tuple2<String,String>, Double>
        >  debug5 = corr.collect();
    for (Tuple2<Tuple2<String,String>, Double> t2 : debug5) {
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
  
  static double calculateCorr(
     Iterable<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> values) {
     
     //int groupSize = value.size(); // length of each vector
     int groupSize = 0; // length of each vector
     int dotProduct = 0; // sum of ratingProd
     int rating1Sum = 0; // sum of rating1
     int rating2Sum = 0; // sum of rating2
     int rating1NormSq = 0; // sum of rating1Squared
     int rating2NormSq = 0; // sum of rating2Squared
     int maxNumOfumRaters1 = 0;  // max of numOfRaters1
     int maxNumOfumRaters2 = 0;  // max of numOfRaters2
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
     
     return correlation(
               groupSize, 
               dotProduct, 
               rating1Sum,
               rating2Sum,
               rating1NormSq,
               rating2NormSq); 
  }
 
  

  static double correlation(
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
}
