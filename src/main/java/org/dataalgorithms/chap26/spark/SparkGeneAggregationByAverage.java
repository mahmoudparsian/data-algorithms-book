package org.dataalgorithms.chap26.spark;

import java.io.FileReader;
import java.io.BufferedReader;
//
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
//
import org.apache.commons.lang.StringUtils;
//
import org.dataalgorithms.util.PairOfDoubleInteger;



/**
 * This class (implemented in Spark API) solves "Gene Aggregation"
 * Algorithm by Averaging Gene values".
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkGeneAggregationByAverage {

   static final Tuple2<String, String> TUPLE_2_NULL = new Tuple2<String, String>("n", "n");
   
   /**
    * Convert all bioset files into a List<biosetFileName>
    *
    * @param biosets is a filename, which holds all bioset files as HDFS entries
    * an example will be:
    *      /biosets/1000.txt
    *      /biosets/1001.txt
    *      ...
    *      /biosets/1408.txt
    */
   private static List<String> toList(String biosets) throws Exception {
      List<String> biosetFiles = new ArrayList<String>();
      BufferedReader in = new BufferedReader(new FileReader(biosets));
      String line = null;
      while ((line = in.readLine()) != null) {
         String aBiosetFile = line.trim();
         biosetFiles.add(aBiosetFile);
      }
      in.close();
      return biosetFiles;
   }
   
   private static JavaRDD<String> readInputFiles(JavaSparkContext ctx,
                                                 String filename)
      throws Exception {
         List<String> biosetFiles = toList(filename);
         int counter = 0;
         JavaRDD[] rdds = new JavaRDD[biosetFiles.size()];
         for (String biosetFileName : biosetFiles) {
            System.out.println("debug1 biosetFileName=" + biosetFileName);
            JavaRDD<String> record = ctx.textFile(biosetFileName);
            rdds[counter] = record;
            counter++;
         }
         JavaRDD<String> allBiosets = ctx.union(rdds);
         return allBiosets.coalesce(9, false);
   } 
   
   private static Map<String,PairOfDoubleInteger>  buildPatientsMap(Iterable<String> values) {
      Map<String, PairOfDoubleInteger> patients = new HashMap<String, PairOfDoubleInteger>(); 
      for (String patientIdAndGeneValue : values) {
          String[] tokens = StringUtils.split(patientIdAndGeneValue, ","); 
          String patientID = tokens[0];
          //tokens[1] = geneValue
          double geneValue = Double.parseDouble(tokens[1]);
          PairOfDoubleInteger pair = patients.get(patientID); 
          if (pair == null) {
             pair = new PairOfDoubleInteger(geneValue, 1);
             patients.put(patientID, pair);
          }
          else {
             pair.increment(geneValue);
          }
      }
      return patients;
   }
  
   private static int getNumberOfPatientsPassedTheTest(Map<String, PairOfDoubleInteger> patients,
                                                       String filterType,
                                                       Double filterValueThreshold) {
      if (patients == null) {
         return 0;
      }
      
      // now, we will average the values and see which patient passes the threshhold
      int passedTheTest = 0;
      for (Map.Entry<String, PairOfDoubleInteger> entry : patients.entrySet()) {
          //String patientID = entry.getKey();
          PairOfDoubleInteger pair = entry.getValue();
          double avg = pair.avg();
          if (filterType.equals("up")) {
              if (avg >= filterValueThreshold) {
                 passedTheTest++;
              }
          }
          if (filterType.equals("down")) {
              if (avg <= filterValueThreshold) {
                 passedTheTest++;
              }
          }
          else if (filterType.equals("abs")) {
              if (Math.abs(avg) >= filterValueThreshold) {
                 passedTheTest++;
              }
          }
      }
      return  passedTheTest;
   }         
  
     
  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: SparkGeneAggregationByIndividual <referenceType> <filterType> <filterValueThreshold> <biosets> ");
      System.exit(1);
    }
    
    final String referenceType = args[0];        // {"r1", "r2", "r3", "r4"}
    final String filterType = args[1];           // {"up", "down", "abs"}
    final Double filterValueThreshold = new Double(args[2]);
    final String biosets = args[3]; 
     
    System.out.println("args[0]: <referenceType>="+referenceType);
    System.out.println("args[1]: <filterType>="+filterType);
    System.out.println("args[2]: <filterValueThreshold>="+filterValueThreshold);
    System.out.println("args[3]: <biosets>="+biosets);

    JavaSparkContext ctx = new JavaSparkContext();

    final Broadcast<String> broadcastVarReferenceType = ctx.broadcast(referenceType);
    final Broadcast<String> broadcastVarFilterType = ctx.broadcast(filterType);
    final Broadcast<Double> broadcastVarFilterValueThreshold = ctx.broadcast(filterValueThreshold);

    JavaRDD<String> records = readInputFiles(ctx, biosets);
    
    // debug1
    List<String> debug1 = records.collect();
    for (String rec : debug1) {
      System.out.println("debug1 => "+ rec);
    }
    
    
    // JavaPairRDD<K, V>, 
    //   where K = "<geneID><,><referenceType>", 
    //         V = "<patientID><,><geneValue>"
    JavaPairRDD<String, String> genes = records.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String record) {
         String[] tokens = StringUtils.split(record, ";");
         String geneIDAndReferenceType = tokens[0];
         String patientIDAndGeneValue = tokens[1];
         String[] arr = StringUtils.split(geneIDAndReferenceType, ",");
         // arr[0] = geneID
         // arr[1] = referenceType   
         // check referenceType and geneValue
         String referenceType = (String) broadcastVarReferenceType.value();         
         if ( arr[1].equals(referenceType) ){ 
               // prepare key-value for reducer and send it to reducer
               return new Tuple2<String, String>(geneIDAndReferenceType, patientIDAndGeneValue); 
         }
         else {
            // otherwise nothing will be counted  
            // later we will filter out these "null" keys  
            return TUPLE_2_NULL; 
         }      
      }
    });
    
    // public JavaPairRDD<K,V> filter(Function<Tuple2<K,V>,Boolean> f)
    // Return a new RDD containing only the elements that satisfy a predicate;
    // If K = "n", then exclude them 
    JavaPairRDD<String,String> filteredGenes = genes.filter(new Function<Tuple2<String,String>,Boolean>() {
      @Override
      public Boolean call(Tuple2<String, String> s) {
         String value = s._1;
         if (value.equals("n")) {
            // exclude null entries
            return false;
         }
         else {
            return true;
         }
      }
    });
    
    
    // genesByID = JavaPairRDD<K, List<V>> 
    //    where K = "<geneID><,><referenceType>"
    //          V = "<patientID><,><geneValue>"
    JavaPairRDD<String, Iterable<String>> genesByID = filteredGenes.groupByKey();


    //mapValues[U](f: (V) => U): JavaPairRDD[K, U]
    // Pass each value in the key-value pair RDD through a map function without 
    // changing the keys; this also retains the original RDD's partitioning.
    JavaPairRDD<String, Integer> frequency = genesByID.mapValues(new Function< Iterable<String>,  // input
                                                                               Integer            // output
                                                                             >() {  
       @Override
       public Integer call(Iterable<String> values) {
          Map<String, PairOfDoubleInteger>  patients = buildPatientsMap(values);          
          String filterType = (String) broadcastVarFilterType.value();
          Double filterValueThreshold = (Double) broadcastVarFilterValueThreshold.value();
          int passedTheTest = getNumberOfPatientsPassedTheTest(patients, filterType, filterValueThreshold);
          return passedTheTest;
       }
    });
    
    
    List<Tuple2<String, Integer>> finalOutput = frequency.collect();
    for (Tuple2<String, Integer> tuple : finalOutput) {
      System.out.println("final output => "+ tuple._1 + ": " + tuple._2);
    }
    
    // done
    ctx.close();
    System.exit(0);
  }
}
