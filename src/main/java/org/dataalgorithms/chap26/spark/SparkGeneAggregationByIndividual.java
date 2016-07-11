package org.dataalgorithms.chap26.spark;

//STEP-0: import required classes and interfaces
import java.util.List;
import java.util.ArrayList;
//
import java.io.FileReader;
import java.io.BufferedReader;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
//
import org.apache.commons.lang.StringUtils;



/**
 * This class (implemented in Spark API) solves "Gene Aggregation"
 * Algorithm by Individual Gene values".
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkGeneAggregationByIndividual {

   static final Tuple2<String, Integer> TUPLE_2_NULL = new Tuple2<String, Integer>("null", 0);   
   
   public static void main(String[] args) throws Exception {
      //STEP-1: handle input parameters
      if (args.length != 4) {
         System.err.println("Usage: SparkGeneAggregationByIndividual <referenceType> <filterType> <filterValueThreshold> <biosets> ");
         System.exit(1);
      }
    
    final String referenceType = args[0];        // {"r1", "r2", "r3", "r4"}
    final String filterType = args[1];           // {"up", "down", "abs"}
    final Double filterValueThreshold = new Double(args[2]);
    final String biosets = args[3]; 
     
    //System.out.println("args[0]: <sparkMasterURL>="+sparkMasterURL);
    System.out.println("args[0]: <referenceType>="+referenceType);
    System.out.println("args[1]: <filterType>="+filterType);
    System.out.println("args[2]: <filterValueThreshold>="+filterValueThreshold);
    System.out.println("args[3]: <biosets>="+biosets);
     
    //STEP-2: create a spark context object 
    JavaSparkContext ctx = new JavaSparkContext();
   
    //STEP-3: broadcast shard variables   
    final Broadcast<String> broadcastVarReferenceType = ctx.broadcast(referenceType);
    final Broadcast<String> broadcastVarFilterType = ctx.broadcast(filterType);
    final Broadcast<Double> broadcastVarFilterValueThreshold = ctx.broadcast(filterValueThreshold);
   
    //STEP-4: create a single JavaRDD from all bioset files         
    JavaRDD<String> records = readInputFiles(ctx, biosets);
    
    // debug1
    List<String> debug1 = records.collect();
    for (String rec : debug1) {
      System.out.println("debug1 => "+ rec);
    }
    
   //STEP-5: map bioset records into JavaPairRDD(K,V) pairs      
    // where K = "<geneID><,><referenceType>", V = 1
    JavaPairRDD<String, Integer> genes = records.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String record) {
         String[] tokens = StringUtils.split(record, ";");
         String geneIDAndReferenceType = tokens[0];
         String patientIDAndGeneValue = tokens[1];
         System.out.println("genes: tokens[0] geneIDAndReferenceType="+geneIDAndReferenceType);
         System.out.println("genes: tokens[1] patientIDAndGeneValue="+patientIDAndGeneValue);
         
         String[] val = StringUtils.split(patientIDAndGeneValue, ",");
         // val[0] = patientID
         // val[1] = geneValue
         System.out.println("genes: patientID  val[0]="+val[0]);
         System.out.println("genes: geneValue  val[1]="+val[1]);
         double geneValue = Double.parseDouble(val[1]);
         String[] arr = StringUtils.split(geneIDAndReferenceType, ",");
         // arr[0] = geneID
         // arr[1] = referenceType   
         // check referenceType and geneValue
         System.out.println("genes:        geneID arr[0]="+arr[0]);
         System.out.println("genes: referenceType arr[1]="+arr[1]);
         System.out.println("genes: referenceType="+referenceType);
         System.out.println("genes: filterValueThreshold="+filterValueThreshold);
         
         String referenceType = (String) broadcastVarReferenceType.value();
         String filterType = (String) broadcastVarFilterType.value();
         Double filterValueThreshold = (Double) broadcastVarFilterValueThreshold.value();

         if ( (arr[1].equals(referenceType)) && (checkFilter(geneValue, filterType, filterValueThreshold)) ){ 
               // prepare key-value for reducer and send it to reducer
               return new Tuple2<String, Integer>(geneIDAndReferenceType, 1); 
         }
         else {
            // otherwise nothing will be counted  
            // later we will filter out these "null" keys  
            return TUPLE_2_NULL; 
         }      
      }
    });
    
    // debug2
    List<Tuple2<String, Integer>> debug2 = genes.collect();
    for (Tuple2<String, Integer> pair : debug2) {
      System.out.println("debug2 => key="+ pair._1 + "\tvalue="+pair._2);
    }
    
    //STEP-6: filter out the redundant RDD elements          
    // public JavaPairRDD<K,V> filter(Function<Tuple2<K,V>,Boolean> f)
    // Return a new RDD containing only the elements that satisfy a predicate;
    // If a counter (i.e., V) is 0, then exclude them 
    JavaPairRDD<String,Integer> filteredGenes = genes.filter(new Function<Tuple2<String,Integer>,Boolean>() {
      public Boolean call(Tuple2<String, Integer> s) {
          int counter = s._2;
          if (counter > 0) {
             return true;
          }
          else {
             return false;
          }
      }
    });
    
    //STEP-7: reduce by Key and sum up the frequency count           
    JavaPairRDD<String, Integer> counts = filteredGenes.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    //STEP-8: prepare the final output        
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<String, Integer> tuple : output) {
      System.out.println("final output => "+ tuple._1 + ": " + tuple._2);
    }
    
    // done
    ctx.close();
    System.exit(0);
  }
  
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
   
   static JavaRDD<String> readInputFiles(JavaSparkContext ctx,
                                      String filename)
      throws Exception {
         List<String> biosetFiles = toList(filename);
       int counter = 0;
       JavaRDD[] rdds = new JavaRDD[biosetFiles.size()];
       for (String biosetFileName : biosetFiles) {
         System.out.println("readInputFiles(): biosetFileName=" + biosetFileName);
         JavaRDD<String> record = ctx.textFile(biosetFileName);
         rdds[counter] = record;
         counter++;
         }
       JavaRDD<String> allBiosets = ctx.union(rdds);
       return allBiosets;
       //return allBiosets.coalesce(9, false);
   } // readInputFiles
   
   //static  boolean checkFilter(double value) {
   static  boolean checkFilter(double value, String filterType, Double filterValueThreshold) {
      if (filterType.equals("abs")) {
         if (Math.abs(value) >= filterValueThreshold) {
            return true;
         }
         else {
            return false;
         }
      }
      if (filterType.equals("up")) {
         if (value >= filterValueThreshold) {
            return true;
         }
         else {
            return false;
         }
      }         
      if (filterType.equals("down")) {
         if (value <= filterValueThreshold) {
            return true;
         }
         else {
            return false;
         }
      }
      return false;      
   }   
   
}
