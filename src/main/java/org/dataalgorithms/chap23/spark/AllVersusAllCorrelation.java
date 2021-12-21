package org.dataalgorithms.chap23.spark;

// STEP-0: import required classes and interfaces
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
//
import org.dataalgorithms.util.SparkUtil;
import org.dataalgorithms.util.InputOutputUtil;
//
import org.dataalgorithms.chap23.correlation.Pearson;
//import org.dataalgorithms.chap23.correlation.Spearman;
import org.dataalgorithms.chap23.correlation.MutableDouble;


/**
 * What does All-vs-All means?
 *
 * Let selected genes be: G = (g1, g2, g3, g4).
 *
 * Then all-vs-all will correlate between the following:
 *
 *  (g1, g2)
 *  (g1, g3)
 *  (g1, g4)
 *  (g2, g3)
 *  (g2, g4)
 *  (g3, g4)
 *
 * Note that pairs (Ga, Gb) are generated if and only if (Ga < Gb).
 *
 * @author Mahmoud Parsian
 *
 */
public class AllVersusAllCorrelation implements java.io.Serializable {
  
   /**
    * Return true if g1 comes before g2 (to make 
    * sure not to create duplicate pair of genes)
    */
   static boolean smaller(String g1, String g2) {
      if (g1.compareTo(g2) < 0){
         return true;
      }
      else {
         return false;
      }   
   }
   

   static Map<String, MutableDouble> toMap(List<Tuple2<String,Double>> list) {
      Map<String, MutableDouble> map = new HashMap<String, MutableDouble>();
      for (Tuple2<String,Double> entry : list) {
          MutableDouble md = map.get(entry._1);
          if (md == null) {
             map.put(entry._1, new MutableDouble(entry._2));
          }
          else {
             md.increment(entry._2);
          }
      }
      return map;
   }
   
   static Map<String, MutableDouble> toMap(Iterable<Tuple2<String,Double>> list) {
      Map<String, MutableDouble> map = new HashMap<String, MutableDouble>();
      for (Tuple2<String,Double> entry : list) {
          MutableDouble md = map.get(entry._1);
          if (md == null) {
             map.put(entry._1, new MutableDouble(entry._2));
          }
          else {
             md.increment(entry._2);
          }
      }
      return map;
   }  
    
   static List<String> toListOfString(Path hdfsFile) throws Exception {
      FSDataInputStream fis = null;
      BufferedReader br = null;
      FileSystem fs = FileSystem.get(new Configuration());      
      List<String> list = new ArrayList<String>();
      try {
         fis = fs.open(hdfsFile);
         br = new BufferedReader(new InputStreamReader(fis));
          String line = null;
          while ((line = br.readLine()) != null) {
             String value = line.trim();
            list.add(value);
          }
       }
       finally {
          InputOutputUtil.close(br);
       }
       return list;
    } 
     
    static JavaRDD<String> readBiosets(JavaSparkContext ctx,
                                     List<String> biosets) {
      int size = biosets.size();
      int counter = 0;
      StringBuilder paths = new StringBuilder();
      for (String biosetFile : biosets) {
         counter++;
         paths.append(biosetFile);
         if (counter < size) {
            paths.append(",");
         }
      }
      JavaRDD<String> rdd = ctx.textFile(paths.toString());   
      return rdd;
   }   
   
   /**
    * record example in bioset: 37761,2,TCGA-MW-A4EC,1.28728316963258
    *                           <GeneID>,<{1,2,3,4}><,><PatientID><,><BiomarkerValue>
    *
    * <all-bioset-ids-as-filename> = a file name which contains biosets IDs (one biosetID per line)
    */
   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length < 2) {
         System.err.println("Usage: AllVersusAllCorrelation  <reference> <all-biomarkers-filename>");
         System.err.println("Usage: AllVersusAllCorrelation  r2  /biomarkers/biomarkers.txt");
         System.exit(1);
      }
      final String reference = args[0]; // {"r1", "r2", "r3", "r4"}
      final String filename = args[1];

      // STEP-2: create a Spark context object
      JavaSparkContext ctx = SparkUtil.createJavaSparkContext("AllVersusAllCorrelation");

      List<String> list = toListOfString(new Path(filename));
            
      // broadcast K and N as global shared objects,
      // which can be accessed from all cluster nodes
      final Broadcast<String> REF = ctx.broadcast(reference); // "r2"
      //final Broadcast<Integer> broadcastN = ctx.broadcast(N);
       
      // STEP-3: read all transactions from HDFS and create the first RDD                   
      JavaRDD<String> biosets = readBiosets(ctx, list);
      biosets.saveAsTextFile("/output/1");

      // JavaRDD<T> filter(Function<T,Boolean> f)
      // Return a new RDD containing only the elements that satisfy a predicate.
      JavaRDD<String> filtered = biosets.filter(new Function<String,Boolean>() {
        @Override
        public Boolean call(String record) {
          String ref = REF.value();
          String[] tokens = record.split(",");
          if (ref.equals(tokens[1]))  {
             return true; // do return these records
          }
          else {
             return false; // do not retrun these records
          }
        }
      });
      filtered.saveAsTextFile("/output/2");
      
      // PairMapFunction<T, K, V>     
      // T => Tuple2<K, V> = Tuple2<gene, Tuple2<patientID, value>>
      //
      JavaPairRDD<String,Tuple2<String,Double>> pairs = filtered.mapToPair(new PairFunction<
          String,                       // T
          String,                       // K = 1234
          Tuple2<String,Double>         // V = <patientID, value>
        >() {
         @Override
         public Tuple2<String,Tuple2<String,Double>> call(String rec) {
            String[] tokens = rec.split(",");
            // tokens[0] = 1234
            // tokens[1] = 2 (this is a ref in {"1", "2", "3", "4"}
            // tokens[2] = patientID
            // tokens[3] = value       
            Tuple2<String,Double> V = new Tuple2<String,Double>(tokens[2], Double.valueOf(tokens[3]));
            return new Tuple2<String,Tuple2<String,Double>>(tokens[0], V);
         }
      });    
      pairs.saveAsTextFile("/output/3");
    
      // STEP-5: group
      JavaPairRDD<String, Iterable<Tuple2<String,Double>>>  grouped = pairs.groupByKey();
      grouped.saveAsTextFile("/output/4");
      // grouped = (K, V)
      // K = gene
      // V = Iterable<Tuple2<patientID,value>>
       grouped.saveAsTextFile("/output/5");
     
           
      
    //<U> JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)
    // Return the Cartesian product of this RDD and another one,
    // that is, the RDD of all pairs of elements (a, b) where a is in this and b is in other.
    JavaPairRDD< Tuple2<String, Iterable<Tuple2<String,Double>>>,
                 Tuple2<String, Iterable<Tuple2<String,Double>>>
               > cart = grouped.cartesian(grouped);
    cart.saveAsTextFile("/output/6");
    // cart = 
    //        (g1, g1), (g1, g2),  (g1, g3), (g1, g4)     
    //        (g2, g1), (g2, g2),  (g2, g3), (g2, g4)     
    //        (g3, g1), (g3, g2),  (g3, g3), (g3, g4)     
    //        (g4, g1), (g4, g2),  (g4, g3), (g4, g4)    
    
    // filter it and keep the ones (Ga, Gb)  if and only if (Ga < Gb).
    // after filtering, we will have:
    // filtered2 =
    //        (g1, g2),  (g1, g3), (g1, g4)     
    //        (g2, g3), (g2, g4)     
    //        (g3, g4)     
    //
    // JavaRDD<T> filter(Function<T,Boolean> f)
    // Return a new RDD containing only the elements that satisfy a predicate.
      JavaPairRDD<Tuple2<String, Iterable<Tuple2<String,Double>>>,
                  Tuple2<String, Iterable<Tuple2<String,Double>>>> filtered2 = 
             cart.filter(new Function<Tuple2<Tuple2<String, Iterable<Tuple2<String,Double>>>,
                                             Tuple2<String, Iterable<Tuple2<String,Double>>>
                                            >,
                        Boolean>() {
        @Override
        public Boolean call(Tuple2<Tuple2<String, Iterable<Tuple2<String,Double>>>,
                                   Tuple2<String, Iterable<Tuple2<String,Double>>>> pair) {
          // pair._1 = Tuple2<String, Iterable<Tuple2<String,Double>>>
          // pair._2 = Tuple2<String, Iterable<Tuple2<String,Double>>>
          if (smaller(pair._1._1,  pair._2._1))  {
             return true; // do return these records
          }
          else {
             return false; // do not retrun these records
          }
        }
      });
      filtered2.saveAsTextFile("/output/7");    
      

      
      // next iterate through all mappedValues
      // JavaPairRDD<String, List<Tuple2<String,Double>>> mappedvalues
      // create (K,V), where 
      //   K = Tuple2<String,String>(g1, g2)
      //   V = Tuple2<Double,Double>(corr, pvalue)
      //    
      JavaPairRDD<Tuple2<String,String>,Tuple2<Double,Double>> finalresult =
            filtered2.mapToPair(new PairFunction<
                Tuple2<Tuple2<String,Iterable<Tuple2<String,Double>>>,
                       Tuple2<String,Iterable<Tuple2<String,Double>>>>, // input
                Tuple2<String,String>,                                  // K
                Tuple2<Double,Double>                                   // V
            >() {
      @Override
      public Tuple2<Tuple2<String,String>,Tuple2<Double,Double>> 
        call(Tuple2<Tuple2<String,Iterable<Tuple2<String,Double>>>,
                    Tuple2<String,Iterable<Tuple2<String,Double>>>> t) {
        Tuple2<String,Iterable<Tuple2<String,Double>>> g1 = t._1;
        Tuple2<String,Iterable<Tuple2<String,Double>>> g2 = t._2;
        // 
        Map<String, MutableDouble> g1map = toMap(g1._2); 
        Map<String, MutableDouble> g2map = toMap(g2._2);
        // now perform a correlation(one, other)
        // make sure we order the values accordingly by patientID
        // each patientID may have one or more values
        List<Double> x = new ArrayList<Double>();
        List<Double> y = new ArrayList<Double>();
        for (Map.Entry<String, MutableDouble> g1Entry : g1map.entrySet()) {
            String g1PatientID = g1Entry.getKey();
            MutableDouble g2MD = g2map.get(g1PatientID);
            if (g2MD != null) {
               // both one and other for patientID have values
               x.add(g1Entry.getValue().avg());
               y.add(g2MD.avg());
            }
        }
        
        System.out.println("x="+x);
        System.out.println("y="+y);
        // K = pair of genes
        Tuple2<String,String> K = new Tuple2<String,String>(g1._1,g2._1);
        if (x.size() < 3) {
            return new Tuple2<Tuple2<String,String>,Tuple2<Double,Double>>
                (
                  K, new Tuple2<Double,Double>(Double.NaN, Double.NaN)
                );
        
        }
        else {
            // Pearson
            double correlation = Pearson.getCorrelation(x, y);  
            double pvalue = Pearson.getPvalue(correlation, x.size() );
        
            // Spearman
            //double correlation = Spearman.getCorrelation(x, y);  
            //double pvalue = Spearman.getPvalue(correlation, (double) x.size() );
            return new Tuple2<Tuple2<String,String>,Tuple2<Double,Double>>
                (
                 K, 
                 new Tuple2<Double,Double>(correlation, pvalue)
                );
        }
      }
    });
    finalresult.saveAsTextFile("/output/corr");          
    
    // done
    ctx.close();
    System.exit(0);
  }

}
