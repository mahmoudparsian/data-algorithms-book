package org.dataalgorithms.chapB04.rankproduct.sparkwithlambda;

import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
//
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
//
import org.apache.log4j.Logger;
//
import org.apache.commons.lang.StringUtils;
//
import org.dataalgorithms.chapB04.rankproduct.util.Util;



/**
 * Description: SparkRankProductUsingCombineByKey
 * NOTE: combineByKey() is used for grouping keys by their associated values.
 *
 * Handles multiple studies, where each study is a set of assays.
 * For each study, we find the mean per gene and then calculate
 * the rank product for all genes.
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkRankProductUsingCombineByKey {

    private static final Logger THE_LOGGER = Logger.getLogger(SparkRankProductUsingCombineByKey.class);

    /**
     * AverageCount is used by combineByKey() to hold 
     * the total values and their count.
     */
    static class AverageCount implements Serializable {
        double total;
        int count;            
        public AverageCount(double total, int count) {
            this.total = total;
            this.count = count;
        }
        public double average() {
            return total / (double) count; 
        }
    } 
    
    /**
     * RankProduct is used by combineByKey() to hold 
     * the total product values and their count.
     */
    static class RankProduct implements Serializable {
        long product;
        int count;            
        public RankProduct(long product, int count) {
            this.product = product;
            this.count = count;
        }
        public double rank() {
            return Math.pow((double) product, 1.0/ (double) count); 
        }
    }        
        
    static void debug(String[] args)  {
        for (int i=0; i < args.length; i++){
            THE_LOGGER.info("***debug*** args["+i+"]="+args[i]);
        }
    }
      
    static void debug(List<Tuple2<String, Tuple2<Double,Boolean>>> list) {
        System.out.println("list="+list);
    }
    

    public static void main(String[] args) throws Exception {
        // args[0] = output path
        // args[1] = number of studies (K)
        // args[2] =   input path for study 1
        // args[3] =   input path for study 2
        // ...
        // args[K+1] = input path for study K
        debug(args);

        final String outputPath = args[0];
        final String numOfStudiesAsString = args[1];
        final int K = Integer.parseInt(numOfStudiesAsString);        
        List<String> inputPathMultipleStudies = new ArrayList<String>();  
        for (int i=1; i <= K; i++) {
            String singleStudyInputPath = args[1+i];
            inputPathMultipleStudies.add(singleStudyInputPath);
        }
        
        boolean useYARN = true;
        performRrankProduct(inputPathMultipleStudies, outputPath, useYARN); 
        System.exit(0);
    }
    
      
    /**
     * 
     * @param inputPathMultipleStudies
     * @param outputPath
     * @param useYARN
     * @throws Exception 
     */
    public static void performRrankProduct(
                                    final List<String> inputPathMultipleStudies, 
                                    final String outputPath, 
                                    final boolean useYARN) 
        throws Exception {   
        
        // create a context object, which is used 
        // as a factory for creating new RDDs
        JavaSparkContext context = Util.createJavaSparkContext(useYARN);
                       
        // Spark requires an array for creating union of many RDDs
        int index = 0;
        JavaPairRDD<String, Double>[] means = new JavaPairRDD[inputPathMultipleStudies.size()];
        for (String inputPathSingleStudy : inputPathMultipleStudies) {
            means[index] = computeMeanByCombineByKey(context, inputPathSingleStudy);
            index++;
        }
                            
        //
        // next compute rank
        // 1. sort values based on absolute value of copa scores: to sort by copa score, we will swap K with V and then sort by key
        // 2. assign rank from 1 (to highest copa score) to n (to the lowest copa score)
        // 3. calcluate rank for each mapped_id as Math.power(R1 * R2 * ... * Rn, 1/n)
        //
        JavaPairRDD<String,Long>[]  ranks = new JavaPairRDD[means.length];
        for (int i=0; i < means.length; i++) {
            ranks[i] = assignRank(means[i]);
        }  
        
        // calculate ranked products
        // <mapped_id, T2<rankedProduct, N>>
        JavaPairRDD<String, Tuple2<Double, Integer>> rankedProducts = 
                computeRankedProductsUsingCombineByKey(context, ranks); 
       
        // save the final output, which is the medianRDD
        // before saving the fianl output, make sure that outputPath does not exist
	deleteDirectoryAndIgnoreException(outputPath);
                 
        // save the result, shuffle=true
        rankedProducts.coalesce(1,true).saveAsTextFile(outputPath); 
        
        // close the context and we are done
        context.close();
    }
    
    /**
     * Delete a directory and all its contents; if exception, ignore it
     */
    static void deleteDirectoryAndIgnoreException(String pathAsString)  throws Exception {
        Path path = new Path(pathAsString);
	FileSystem fs = FileSystem.get(new Configuration());
	fs.delete(path, true);	
    }

    
    // JavaPairRDD<String, Tuple2<Double, Integer>> = <mapped_id, T2(rankedProduct, N>>
    // where N is the number of elements for computing the rankedProduct
    static JavaPairRDD<String, Tuple2<Double, Integer>> computeRankedProductsUsingCombineByKey(
            JavaSparkContext context,
            JavaPairRDD<String, Long>[] ranks) {
        JavaPairRDD<String, Long> unionRDD = context.union(ranks);
        
        //
        // we need 3 function to be able to use combinebyKey()
        //
        // function 1
        Function<Long, RankProduct> createCombiner = (Long x) -> new RankProduct(x, 1);
        
        // function 2
        Function2<RankProduct, Long, RankProduct> addAndCount =
          (RankProduct a, Long x) -> {
              a.product *= x;
              a.count += 1;
              return a;
        };
        
        // function 3
        Function2<RankProduct, RankProduct, RankProduct> mergeCombiners =
          (RankProduct a, RankProduct b) -> {
              a.product *= b.product;
              a.count += b.count;
              return a;
        };
        
        // next find unique keys, with their associated copa scores
        JavaPairRDD<String, RankProduct> combinedByGeneRDD =
              unionRDD.combineByKey(createCombiner, addAndCount, mergeCombiners);        
                
        // next calculate ranked products and the number of elements
        JavaPairRDD<String, Tuple2<Double, Integer>> rankedProducts = 
                combinedByGeneRDD.mapValues((RankProduct value) -> {
            double theRankedProduct = value.rank();
            return new Tuple2<Double, Integer>(theRankedProduct, value.count);
        }); 
        //
        return rankedProducts;
    }    
    
     /**
     * Compute mean per gene for a single study = set of assays
     * @param context an instance of JavaSparkContext
     * @param inputPath set of assay HDFS paths separated by ","
     */
    static JavaPairRDD<String, Double> computeMeanByCombineByKey( 
                                JavaSparkContext context,           
                                final String inputPath)
        throws Exception {    
        
        JavaPairRDD<String, Double> genes = getGenesUsingTextFile(context, inputPath, 30);        
        //JavaPairRDD<String, Double> genes = getGenesUsingCustomCombineFileInputFormat(context, inputPath);        

        //
        // we need 3 function to be able to use combinebyKey()
        //
        // function 1
        Function<Double, AverageCount> createCombiner = (Double x) -> new AverageCount(x, 1);
        
        // function 2
        Function2<AverageCount, Double, AverageCount> addAndCount =
          (AverageCount a, Double x) -> {
              a.total += x;
              a.count += 1;
              return a;
        };
        
        // function 3
        Function2<AverageCount, AverageCount, AverageCount> mergeCombiners =
          (AverageCount a, AverageCount b) -> {
              a.total += b.total;
              a.count += b.count;
              return a;
        };
        
        JavaPairRDD<String, AverageCount> averageCounts =
          genes.combineByKey(createCombiner, addAndCount, mergeCombiners);
        
        // debug
        Map<String, AverageCount> countMap = averageCounts.collectAsMap();
        for (Entry<String, AverageCount> entry : countMap.entrySet()) {
          System.out.println(entry.getKey() + ":" + entry.getValue().average());
        }
        
        // now compute the mean/average per gene
        JavaPairRDD<String,Double> meanRDD = 
                averageCounts.mapToPair((Tuple2<String, AverageCount> s) -> 
                new Tuple2<String,Double>(s._1, s._2.average()) 
        );
                
        return meanRDD;
    }      
    
     /**
     * Create JavaPairRDD<String, Double> = JavaPairRDD<mapped_id, test_expression>
     * This method worked very well with LARGE set of input paths. 
     * This method combines small input files into a large file 
     * size CustomCombineFileInputFormat.MAX_SPLIT_SIZE
     * 
     * @param context an instance of JavaSparkContext
     * @param inputPath "hdfs_path_1,hdfs_path_2, ...,hdfs_path_N"
     * @return JavaPairRDD<String, Double>
     * @throws Exception failed to create JavaPairRDD
     */
    /*
    static JavaPairRDD<String, Double> getGenesUsingCustomCombineFileInputFormat(
            JavaSparkContext context,
            String inputPath) throws Exception {   
        //public <K,V,F extends org.apache.hadoop.mapreduce.InputFormat<K,V>> 
        //  JavaPairRDD<K,V> newAPIHadoopFile(String path,
        //                                    Class<F> fClass,
        //                                    Class<K> kClass,
        //                                    Class<V> vClass,
        //                                    org.apache.hadoop.conf.Configuration conf)
        Configuration hadoopConfiguration = new Configuration();
        JavaPairRDD<Text, DoubleWritable> genesAsHadoopRDD = 
                context.newAPIHadoopFile(inputPath,                             // String path,
                                         CustomCombineFileInputFormat.class,    // Class<F> fClass,
                                         Text.class,                            // kClass,
                                         DoubleWritable.class,                  // vClass,
                                         hadoopConfiguration);                  // org.apache.hadoop.conf.Configuration conf

        // the following map() is an identity mapper:
        // it just does data type conversion:
        //      1. converts Key/Text to String
        //      2. converts Value/DoubleWritable to Double
        //
        // PairFunction<T, K, V>	T => Tuple2<K, V>
        JavaPairRDD<String,Double> genes = genesAsHadoopRDD.mapToPair(
            new PairFunction<
                             Tuple2<Text, DoubleWritable>, // T: input
                             String,                       // K
                             Double                        // V
                            >() {
            @Override
            public Tuple2<String,Double> call(Tuple2<Text, DoubleWritable> s) {
                return new Tuple2<String,Double>(s._1.toString(), s._2.get());
            }
        });
        
        return genes;
    }
    */
    
    // result is JavaPairRDD<String, Long> = (mapped_id, rank)
    static JavaPairRDD<String,Long>  assignRank(JavaPairRDD<String,Double> rdd) 
        throws Exception {
        
        // swap key and value (will be used for sorting by key)
        // convert value to abs(value)
        JavaPairRDD<Double,String> swappedRDD = 
                rdd.mapToPair((Tuple2<String, Double> s) -> 
                    new Tuple2<Double,String>(Math.abs(s._2), s._1) 
        ); 
        
        // sort copa scores descending
        // we need 1 partition so that we can zip numbers into this RDD by zipWithIndex()
        JavaPairRDD<Double,String> sorted = swappedRDD.sortByKey(false, 1);
        
        // JavaPairRDD<T,Long> zipWithIndex()
        // Long values will be 0, 1, 2, ...
        // for ranking, we need 1, 2, 3, ..., therefore, we will add 1 when calculating the ranked product
        JavaPairRDD<Tuple2<Double,String>,Long> indexed = sorted.zipWithIndex();
        
        // next convert JavaPairRDD<Tuple2<Double,String>,Long> into JavaPairRDD<String,Long>
        //              JavaPairRDD<Tuple2<value,mapped_id>,rank> into JavaPairRDD<mapped_id,rank>
        //
        // K: mapped_id
        // V: rank
        // ranks are 1, 2, ..., n
        JavaPairRDD<String, Long> ranked = 
                indexed.mapToPair((Tuple2<Tuple2<Double,String>,Long> s) -> 
                        new Tuple2<String,Long>(s._1._2, s._2 + 1)   
        );  
        //
        return ranked;
    }
    
    
    /**
     * Create JavaPairRDD<String, Double> = JavaPairRDD<mapped_id, test_expression>
     * This method worked well with small set of input paths.
     * When you pass a large number of input paths, this method takes a VERY LONG time to complete.
     * 
     * @param context an instance of JavaSparkContext
     * @param inputPath "hdfs_path_1,hdfs_path_2, ...,hdfs_path_N"
     * @return JavaPairRDD<String, Double>
     * @throws Exception failed to create JavaPairRDD
     */
    static JavaPairRDD<String, Double> getGenesUsingTextFile(
            JavaSparkContext context,
            String inputPath,
            int numberOfPartitions) throws Exception {
    
        // read input and create the first RDD
        // JavaRDD<String>: where String = "mapped_id,test_expression"
        // JavaRDD<String> records = context.textFile(inputPath, 1).coalesce(3); // WORKED
        JavaRDD<String> records = context.textFile(inputPath, numberOfPartitions); // WORKED
                          
        // for each record, we emit (K=mapped_id, V=test_expression)
        JavaPairRDD<String, Double> genes
                = records.mapToPair((String rec) -> {
                    // rec = "mapped_id,test_expression"
                    String[] tokens = StringUtils.split(rec, ",");
                    // tokens[0] = mapped_id
                    // tokens[1] = test_expression
                    return new Tuple2<String, Double>(tokens[0], Double.parseDouble(tokens[1]));
        });
        
        return genes;
    }
   
}
