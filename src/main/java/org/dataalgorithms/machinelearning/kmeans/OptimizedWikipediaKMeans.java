package org.dataalgorithms.machinelearning.kmeans;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
//
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
//
import java.text.SimpleDateFormat;
//
import org.apache.commons.io.IOUtils;
//
import org.apache.log4j.Logger;
//
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
//
import scala.Tuple2;

/**
 * NOTE: ----------------------------------------
 *       Apache Spark provides distributed K-Means algorithm; 
 *       the purpose of this class is to exercise and understand 
 *       how does K-Means work.
 * END NOTE --------------------------------------
 * 
 * @author Khatwani Parth Bharat (h2016170@pilani.bits-pilani.ac.in)
 *
 * @editor Mahmoud Parsian (mahmoud.parsian@yahoo.com)
 *
 *
 */
public class OptimizedWikipediaKMeans {

    private static final Logger THE_LOGGER = Logger.getLogger(OptimizedWikipediaKMeans.class);

    static Vector average(Vector vec, Integer numVectors) {
        double[] avg = new double[vec.size()];
        for (int i = 0; i < avg.length; i++) {
            // avg[i] = vec.apply(i) * (1.0 / numVectors);
            avg[i] = vec.apply(i) / ((double) numVectors);
        }
        return new DenseVector(avg);
    }

    static JavaRDD<Vector> getFeatureizedData(String wikiData, JavaSparkContext context) {
        JavaRDD<Vector> data = context.textFile(wikiData).map(new Function<String, Vector>() {
            @Override
            public Vector call(String arg0) throws Exception {
                return Util.buildVector(arg0, "\t");
            }
        }).cache();
        return data;
    }

    static Map<Integer, Vector> getNewCentroids(JavaPairRDD<Integer, Tuple2<Vector, Integer>> pointsGroup) {
        Map<Integer, Vector> newCentroids = pointsGroup.mapValues(new Function<Tuple2<Vector, Integer>, Vector>() {
            @Override
            public Vector call(Tuple2<Vector, Integer> arg0) throws Exception {
                return average(arg0._1, arg0._2);
            }
        }).collectAsMap();
        return newCentroids;
    }

    static JavaPairRDD<Integer, Tuple2<Vector, Integer>> getClosest(JavaRDD<Vector> data,
            final List<Vector> centroids) {
        JavaPairRDD<Integer, Tuple2<Vector, Integer>> closest = data
                .mapToPair(new PairFunction<Vector, Integer, Tuple2<Vector, Integer>>() {
                    @Override
                    public Tuple2<Integer, Tuple2<Vector, Integer>> call(Vector in) throws Exception {
                        return new Tuple2<Integer, Tuple2<Vector, Integer>>(Util.closestPoint(in, centroids),
                                new Tuple2<Vector, Integer>(in, 1));
                    }
                });

        return closest;
    }

    static List<Vector> getInitialCentroids(JavaRDD<Vector> data, final int K) {
        List<Vector> centroidTuples = data.take(K);
        final List<Vector> centroids = new ArrayList<Vector>();
        for (Vector t : centroidTuples) {
            centroids.add(t);
        }
        return centroids;
    }
    


    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: OptimizedWikipediaKMeans <file> <k> <iters> <outputfiletime_details> <errorLogFile> <outputfile>");
            System.exit(1);
        }
        //
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-YY HH:mm");
        long progSTime = System.currentTimeMillis();
        PrintWriter errorWriter = null;
        BufferedWriter writer = null;
        BufferedWriter outputWriter = null;
        JavaSparkContext context = null;
        try {
            context = new JavaSparkContext(); // "OptimizedWikipediaKMeans"
            final String wikiData = args[0];
            final int K = Integer.parseInt(args[1]);
            final double convergeDist = Double.parseDouble(args[2]);
            //
            writer = new BufferedWriter(new FileWriter(args[3] + sdf.format(new Date()) + ".txt"));
            errorWriter = new PrintWriter(new FileWriter(args[4] + sdf.format(new Date()) + ".txt"));
            outputWriter = new BufferedWriter(new FileWriter(args[5] + sdf.format(new Date()) + ".txt"));
            //
            long dataReadSTime = System.currentTimeMillis();
            //read the input data
            JavaRDD<Vector> data = getFeatureizedData(wikiData, context);
            THE_LOGGER.info("Number of data records " + data.count());
            //get the initail k centriods			
            final List<Vector> centroids = getInitialCentroids(data, K);
            THE_LOGGER.info("Done selecting initial centroids: " + centroids.size());
            long dataReadETime = System.currentTimeMillis();
            writer.write("Total Data Read time is " + (((double) (dataReadETime - dataReadSTime)) / 1000.0) + "\n");
            System.out.println("Total Data Read time is " + (((double) (dataReadETime - dataReadSTime)) / 1000.0) + "\n");
            double tempDist = 1.0 + convergeDist;
            long clusteringSTime = System.currentTimeMillis();
            long itrSTime = System.currentTimeMillis();
            int i = 0;
            while (tempDist > convergeDist) {
                System.out.println("Get the closest points");
                //assign each point to their closest centriod 				
                JavaPairRDD<Integer, Tuple2<Vector, Integer>> closest = getClosest(data, centroids);
                // reduce step
                JavaPairRDD<Integer, Tuple2<Vector, Integer>> pointsGroup = closest.reduceByKey(
                        new Function2<Tuple2<Vector, Integer>, Tuple2<Vector, Integer>, Tuple2<Vector, Integer>>() {
                    @Override
                    public Tuple2<Vector, Integer> call(Tuple2<Vector, Integer> arg0,
                            Tuple2<Vector, Integer> arg1) throws Exception {
                        return new Tuple2<Vector, Integer>(Util.add(arg0._1(), arg1._1()), arg0._2() + arg1._2());
                    }
                });
                System.out.println("after reduce by key");
                //get the new centriods				
                Map<Integer, Vector> newCentroids = getNewCentroids(pointsGroup);
                System.out.println("after getting new centriods");
                //calculate the delta					
                tempDist = Util.getDistance(centroids, newCentroids, K);
                System.out.println("after updating the distance");
                //assign new centriods
                for (Map.Entry<Integer, Vector> t : newCentroids.entrySet()) {
                    centroids.set(t.getKey(), t.getValue());
                }
                System.out.println("after updating centriod list");
                long itrETime = System.currentTimeMillis();
                writer.write("Iteration" + (i + 1) + " took :- " + (((double) (itrETime - itrSTime)) / 1000.0) + "\n");
                System.out.println("Iteration" + (i + 1) + " took :- " + (((double) (itrETime - itrSTime)) / 1000.0) + "\n");
                THE_LOGGER.info("Finished iteration (delta = " + tempDist + ")");
                itrSTime = itrETime;
                i++;
                System.out.println("loop ends");
            }
            long clusteringETime = System.currentTimeMillis();
            writer.write("Clustering time is " + ((double) (clusteringETime - clusteringSTime)) / (1000.0) + "\n");
            System.out.println("Clustering time is " + ((double) (clusteringETime - clusteringSTime)) / (1000.0) + "\n");
            long progETime = System.currentTimeMillis();
            writer.write("Total Program time is " + (((double) (progETime - progSTime)) / 1000.0) + "\n");
            System.out.println("Total Program time is " + (((double) (progETime - progSTime)) / 1000.0) + "\n");
            //
            THE_LOGGER.info("Cluster centriods:");
            outputWriter.write("Cluster centriods:\n");
            for (Vector t : centroids) {
                //System.out.println("" + t.apply(0)+"\t"+t.apply(1)+"\t"+t.apply(2));
                outputWriter.write("" + t.apply(0) + " " + t.apply(1) + " " + t.apply(2) + "\n");
            }
        } 
        finally {
            IOUtils.closeQuietly(writer);
            IOUtils.closeQuietly(errorWriter);
            IOUtils.closeQuietly(outputWriter);
            System.out.println("Stoping context");
            if (context != null) {
                context.stop();
            }
        }
        //
        System.exit(0);
    }
}
