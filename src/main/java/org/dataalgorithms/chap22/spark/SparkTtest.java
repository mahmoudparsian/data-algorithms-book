package org.dataalgorithms.chap22.spark;

// STEP-0: import required classes and interfaces
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
//
import java.io.FileReader;
import java.io.BufferedReader;
//
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
//
import org.apache.commons.lang.StringUtils;
//
import org.dataalgorithms.util.MathUtil;
import org.dataalgorithms.util.SparkUtil;

/**
 * This class implements Ttest using Spark API.
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkTtest {

   static Map<String, Double> createTimeTable(String filename) throws Exception {
      Map<String, Double> map = new HashMap<String, Double>();
      BufferedReader in = null;
      try {
         in = new BufferedReader(new FileReader(filename));
         String line = null;
         while ((line = in.readLine()) != null) {
            String value = line.trim();
            String[] tokens = value.split("\t");
            String biosetID = tokens[0];
            Double time = new Double(tokens[1]);
            map.put(biosetID, time);
         }
         System.out.println("createTimeTable() map="+map);
      }
      finally {
         if (in != null) {
           in.close();
         }
      }
      return map;
   }
   
   static JavaRDD<String> readBiosetFiles(JavaSparkContext ctx,
                                          String biosetFiles)
      throws Exception {
      StringBuilder  unionPath = new StringBuilder();
      BufferedReader in = null;
      try {
         in = new BufferedReader(new FileReader(biosetFiles));
         String singleBiosetFile = null;
         while ((singleBiosetFile = in.readLine()) != null) {
            singleBiosetFile = singleBiosetFile.trim();
            unionPath.append(singleBiosetFile);
            unionPath.append(",");
         }
         //System.out.println("readBiosetFiles() unionPath="+unionPath);
      }
      finally {
         if (in != null) {
           in.close();
         }
      }
      // remove the last comma ","
      String unionPathAsString = unionPath.toString();
      unionPathAsString = unionPathAsString.substring(0, unionPathAsString.length()-1);
      // create RDD   
      JavaRDD<String> allBiosets = ctx.textFile(unionPathAsString);
      JavaRDD<String> partitioned = allBiosets.coalesce(14);
      return partitioned;
   }
   

   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length != 3) {
         System.err.println("Usage: SparkTtest <timetable-file> <bioset-file> <yarn-resourcemanager>");
         System.exit(1);
      }
      String timetableFileName = args[0];
      System.out.println("<timetable-file>="+timetableFileName);
      String biosetFileNames = args[1];
      System.out.println("<bioset-file>="+biosetFileNames);
      String yarnResourceManager = args[2];
         
      // STEP-2: Create time table data structure
      Map<String, Double> timetable = createTimeTable(timetableFileName);

      // STEP-3: create a spark context object 
      JavaSparkContext ctx =   SparkUtil.createJavaSparkContext(yarnResourceManager);  

      // STEP-4: broadcast shard variables  used by all cluster nodes
      // we need this shared data structure when we want to find
      // "existSet" and notExistSet" sets after grouping data by GENE-ID.
      final Broadcast<Map<String, Double>> broadcastTimeTable = ctx.broadcast(timetable);
    
      // STEP-5: create RDD for all biosets
      // each RDD element has: <geneID><,><biosetID><,><geneValue>
      JavaRDD<String> biosets = readBiosetFiles(ctx, biosetFileNames);
      biosets.saveAsTextFile("/ttest/output/1");   

      // STEP-6: map bioset records into JavaPairRDD(K,V) pairs      
      // where K = <GeneID>
      //       V = <Bioeset-ID>
      // Note that for TTest, <geneValue> is not used (ignored here)
      //                                                                     T       K       V
      JavaPairRDD<String, String> pairs = biosets.mapToPair(new PairFunction<String, String, String>() {
         @Override
         public Tuple2<String, String> call(String biosetRecord) {
            String[] tokens = StringUtils.split(biosetRecord, ",");
            String geneID = tokens[0];   // K
            String biosetID = tokens[1]; // V
            return new Tuple2<String,String>(geneID, biosetID);
         }
      });
      pairs.saveAsTextFile("/ttest/output/2");   

      // STEP-7: group biosets by GENE-ID    
      JavaPairRDD<String, Iterable<String>> grouped = pairs.groupByKey();
      // now, for each GENE-ID we have a List<BIOSET-ID>
      grouped.saveAsTextFile("/ttest/output/3");   
      
      // STEP-8: perform Ttest for every GENE-ID
      // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
      // Pass each value in the key-value pair RDD through a map function without 
      // changing the keys; this also retains the original RDD's partitioning.
      JavaPairRDD<String, Double> ttest =  grouped.mapValues(
          new Function<Iterable<String>,  // input
                       Double             // output (result of ttest)
          >() {  
          @Override
          public Double call(Iterable<String> biosets) {
             Set<String> geneBiosets = new HashSet<String>();
             for (String biosetID : biosets) {
                geneBiosets.add(biosetID);
             }
             
             // now we do need shared Map data structure to iterate over its items
             Map<String, Double> timetable = broadcastTimeTable.value();
             // the following two lists are needed for ttest(exist, notexist)
             List<Double> exist = new ArrayList<Double>();  
             List<Double> notexist = new ArrayList<Double>();  
             for (Map.Entry<String, Double> entry : timetable.entrySet()) {
                 String biosetID = entry.getKey();
                 Double time = entry.getValue();
                 if (geneBiosets.contains(biosetID)) {
                    exist.add(time);
                 }
                 else {
                    notexist.add(time);
                 }
             }
             
             // perform the ttest(exist, notexist)
             double ttest = MathUtil.ttest(exist, notexist);
             return ttest;
          }
      });

      ttest.saveAsTextFile("/ttest/output/4");   
      
      // done
      ctx.close();
      System.exit(0);
   }
   
}
