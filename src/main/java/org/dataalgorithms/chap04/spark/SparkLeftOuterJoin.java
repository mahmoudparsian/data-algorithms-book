package org.dataalgorithms.chap04.spark;

// STEP-0: import required classes and interfaces
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;


/**
 * This class provides a basic implementation of "left outer join"  
 * operation for a given two tables.  This class is provided as  
 * an educational tool to understand the concept of "left outer join" 
 * functionality.  
 *
 *
 * Note that Spark API does provide JavaPairRDD.leftOuterJoin() functionality.
 *
 * @author Mahmoud Parsian
 *
 */ 
public class SparkLeftOuterJoin {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
       System.err.println("Usage: SparkLeftOuterJoin <users> <transactions>");
       System.exit(1);
    }
    String usersInputFile = args[0];
    String transactionsInputFile = args[1];
    System.out.println("users="+ usersInputFile);
    System.out.println("transactions="+ transactionsInputFile);
       
    JavaSparkContext ctx = new JavaSparkContext();
					
    JavaRDD<String> users = ctx.textFile(usersInputFile, 1);

    // mapToPair
    // <K2,V2> JavaPairRDD<K2,V2> mapToPair(PairFunction<T,K2,V2> f)
    // Return a new RDD by applying a function to all elements of this RDD.
    // PairFunction<T, K, V>	
    // T => Tuple2<K, V>
    JavaPairRDD<String,Tuple2<String,String>> usersRDD = 
          users.mapToPair(new PairFunction<
                                           String,                // T 
                                           String,                // K
                                           Tuple2<String,String>  // V
                                          >() {
      @Override
      public Tuple2<String,Tuple2<String,String>> call(String s) {
      	String[] userRecord = s.split("\t");
      	Tuple2<String,String> location = new Tuple2<String,String>("L", userRecord[1]);
        return new Tuple2<String,Tuple2<String,String>>(userRecord[0], location);
      }
    });
    
    JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);

	// PairFunction<T, K, V>	
	// T => Tuple2<K, V>
    JavaPairRDD<String,Tuple2<String,String>> transactionsRDD = 
          //                                T       K       V
          transactions.mapToPair(new PairFunction<String, String, Tuple2<String,String>>() {
      @Override
      public Tuple2<String,Tuple2<String,String>> call(String s) {
      	String[] transactionRecord = s.split("\t");
      	Tuple2<String,String> product = new Tuple2<String,String>("P", transactionRecord[1]);
        return new Tuple2<String,Tuple2<String,String>>(transactionRecord[2], product);
      }
    });
    
    // here we perform a union() on usersRDD and transactionsRDD
    JavaPairRDD<String,Tuple2<String,String>> allRDD = transactionsRDD.union(usersRDD);
    
    // group allRDD by userID
    JavaPairRDD<String, Iterable<Tuple2<String,String>>> groupedRDD = allRDD.groupByKey(); 
    // now the groupedRDD entries will be as:
    // <userID, List[T2("L", location), T2("P", p1), T2("P", p2), T2("P", p3), ...]>    
   
    // PairFlatMapFunction<T, K, V>	
    // T => Iterable<Tuple2<K, V>>
    JavaPairRDD<String,String> productLocationsRDD = 
         //                                               T                                                K       V 
         groupedRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String,String>>>, String, String>() {
      @Override
      public Iterator<Tuple2<String,String>> call(Tuple2<String, Iterable<Tuple2<String,String>>> s) {
      	// String userID = s._1;  // NOT Needed
      	Iterable<Tuple2<String,String>> pairs = s._2;
       	String location = "UNKNOWN";
       	List<String> products = new ArrayList<String>();
       	for (Tuple2<String,String> t2 : pairs) {
       		if (t2._1.equals("L")) {
       			location = t2._2;
       		}
       		else {
       			// t2._1.equals("P")
       			products.add(t2._2);
       		}
       	}
       	
       	// now emit (K, V) pairs
       	List<Tuple2<String,String>> kvList = new ArrayList<Tuple2<String,String>>();
       	for (String product : products) {
       		kvList.add(new Tuple2<String, String>(product, location));
       	}
       	// Note that edges must be reciprocal, that is every
		// {source, destination} edge must have a corresponding {destination, source}.
        return kvList.iterator();
      }
    });
    
    // Find all locations for a product
	JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();    
    
    // debug3
    List<Tuple2<String, Iterable<String>>> debug3 = productByLocations.collect();
    System.out.println("--- debug3 begin ---");
    for (Tuple2<String, Iterable<String>> t2 : debug3) {
      System.out.println("debug3 t2._1="+t2._1);
      System.out.println("debug3 t2._2="+t2._2);
    }
    System.out.println("--- debug3 end ---");
    
    
    
    JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = 
          productByLocations.mapValues(new Function< Iterable<String>,                   // input
                                                     Tuple2<Set<String>, Integer>        // output
                                                   >() {
      @Override
      public Tuple2<Set<String>, Integer> call(Iterable<String> s) {
      	Set<String> uniqueLocations = new HashSet<String>();
        for (String location : s) {
        	uniqueLocations.add(location);
        }
        return new Tuple2<Set<String>, Integer>(uniqueLocations, uniqueLocations.size());
      }
    });    
    
     // debug4
    System.out.println("=== Unique Locations and Counts ===");
    List<Tuple2<String, Tuple2<Set<String>, Integer>>>  debug4 = productByUniqueLocations.collect();
    System.out.println("--- debug4 begin ---");
    for (Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
      System.out.println("debug4 t2._1="+t2._1);
      System.out.println("debug4 t2._2="+t2._2);
    }
    System.out.println("--- debug4 end ---");
    //productByUniqueLocations.saveAsTextFile("/left/output");
    System.exit(0);
  }
}
