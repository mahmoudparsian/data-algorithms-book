package org.dataalgorithms.chapB04.rankproduct.spark;


import scala.Tuple2;
//
import java.util.List;
import java.util.ArrayList;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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
 * Description: SparkRankProductUsingGroupByKey
 * NOTE: groupByKey() is used for grouping keys by their associated values.
 * 
 * Handles multiple studies, where each study is a set of assays.
 * For each study, we find the mean per gene and then calculate
 * the rank product for all genes.
 *
 * @author Mahmoud Parsian
 *
 */
public class SparkRankProductUsingGroupByKey {

    private static final Logger THE_LOGGER = Logger.getLogger(SparkRankProductUsingGroupByKey.class);
        
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
            means[index] = computeMeanByGroupByKey(context, inputPathSingleStudy);
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
        JavaPairRDD<String, Tuple2<Double, Integer>> rankedProducts = computeRankedProducts(context, ranks); 
       
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
    static JavaPairRDD<String, Tuple2<Double, Integer>> computeRankedProducts(
            JavaSparkContext context,
            JavaPairRDD<String, Long>[] ranks) {
        JavaPairRDD<String, Long> unionRDD = context.union(ranks);
        
        // next find unique keys, with their associated copa scores
        JavaPairRDD<String, Iterable<Long>> groupedByGeneRDD = unionRDD.groupByKey();
        
        // next calculate ranked products and the number of elements
        JavaPairRDD<String, Tuple2<Double, Integer>> rankedProducts = groupedByGeneRDD.mapValues(
                new Function<
                             Iterable<Long>,            // input: copa scores for all studies
                             Tuple2<Double, Integer>    // output: (rankedProduct, N)
                            >() {
                @Override
                public Tuple2<Double, Integer> call(Iterable<Long> values) {
                    int N = 0;
                    long products = 1;
                    for (Long v : values) {
                        products *= v;
                        N++;
                    }
                    
                    double rankedProduct = Math.pow( (double) products, 1.0/((double) N));
                    return new Tuple2<Double, Integer>(rankedProduct, N);
                }
        });                
        return rankedProducts;
    }    
    
    /**
     * Compute mean per gene for a single study = set of assays
     * @param context an instance of JavaSparkContext
     * @param inputPath set of assay HDFS paths separated by ","
     */
    static JavaPairRDD<String, Double> computeMeanByGroupByKey( 
                                JavaSparkContext context,           
                                final String inputPath)
        throws Exception {    
        
        JavaPairRDD<String, Double> genes = getGenesUsingTextFile(context, inputPath, 30);        
        //JavaPairRDD<String, Double> genes = getGenesUsingCustomCombineFileInputFormat(context, inputPath);        

        // group values by gene
        JavaPairRDD<String, Iterable<Double>> groupedByGene = genes.groupByKey();
        
        // calclulate mean per gene
        JavaPairRDD<String, Double> meanRDD = groupedByGene.mapValues(
                new Function<
                             Iterable<Double>,      // input
                             Double                 // output: mean
                            >() {
                @Override
                public Double call(Iterable<Double> values) {
                    double sum = 0.0;
                    int count = 0;
                    for (Double v : values) {
                        sum += v;
                        count++;
                    }
                    // calculate mean of samples
                    double mean = sum / ((double) count);
                    return mean;
                }
        });
        
        return meanRDD;
    }  
    
    // result is JavaPairRDD<String, Long> = (mapped_id, rank)
    static JavaPairRDD<String,Long>  assignRank(JavaPairRDD<String,Double> rdd) 
        throws Exception {
        
        // swap key and value (will be used for sorting by key)
        // convert value to abs(value)
        JavaPairRDD<Double,String> swappedRDD = rdd.mapToPair(
            new PairFunction<
                             Tuple2<String, Double>,       // T: input
                             Double,                       // K
                             String                        // V
                            >() {
            @Override
            public Tuple2<Double,String> call(Tuple2<String, Double> s) {
                return new Tuple2<Double,String>(Math.abs(s._2), s._1);
            }
        }); 
        
        // sort copa scores descending
        // we need 1 partition so that we can zip numbers into this RDD by zipWithIndex()
        JavaPairRDD<Double,String> sorted = swappedRDD.sortByKey(false, 1);
        
        // JavaPairRDD<T,Long> zipWithIndex()
        // Long values will be 0, 1, 2, ...
        // for ranking, we need 1, 2, 3, ..., therefore, we will add 1 when calculating the ranked product
        JavaPairRDD<Tuple2<Double,String>,Long> indexed = sorted.zipWithIndex();
        
        // next convert JavaPairRDD<Tuple2<Double,String>,Long> into JavaPairRDD<String,Long>
        //              JavaPairRDD<Tuple2<value,mapped_id>,rank> into JavaPairRDD<mapped_id,rank>
        JavaPairRDD<String, Long> ranked = indexed.mapToPair(
            new PairFunction<
                             Tuple2<Tuple2<Double,String>,Long>,  // T: input
                             String,                              // K: mapped_id
                             Long                                 // V: rank
                            >() {
            @Override
            public Tuple2<String, Long> call(Tuple2<Tuple2<Double,String>,Long> s) {
                return new Tuple2<String,Long>(s._1._2, s._2 + 1); // ranks are 1, 2, ..., n
            }
        });   
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
                = records.mapToPair(new PairFunction<String, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(String rec) {
                        // rec = "mapped_id,test_expression"
                        String[] tokens = StringUtils.split(rec, ",");
                        // tokens[0] = mapped_id
                        // tokens[1] = test_expression
                        return new Tuple2<String, Double>(tokens[0], Double.parseDouble(tokens[1]));
                    }
        });
        
        return genes;
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
    
    static void debugit(JavaPairRDD<String, Iterable<Double>> groupedByGeneFiltered) throws Exception {      
        //// BEGIN DEBUG
        //// BEGIN DEBUG
        JavaPairRDD<String, List<Double>> debugRDD =
            groupedByGeneFiltered.mapValues(new Function<
                                                         Iterable<Double>, // input
                                                         List<Double>      // output
                                                        >() {
            @Override
            public List<Double> call(Iterable<Double> values) {
                // build list of test_expression 
                List<Double> list = new ArrayList<Double>();
                for (Double d : values) {
                    list.add(d);
                }  
                return list;
            }
        }); 
        String debugPath = "/biomarker/output/rnae/debug";
 	deleteDirectoryAndIgnoreException(debugPath);
        debugRDD.coalesce(1,true).saveAsTextFile(debugPath);      
        //// END DEBUG
        //// END DEBUG       
    }
    
}
