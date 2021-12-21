package org.dataalgorithms.chapB11.cartesian.spark;

import java.util.List;
import java.util.ArrayList;
//
import scala.Tuple2;
//
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Basic Spark program to demonstrate rdd.cartesian(rdd) in Java.
 *
 * @author Mahmoud Parsian
 *
 */
public class TestCartesian {

   static JavaSparkContext createJavaSparkContext() throws Exception {
      SparkConf conf = new SparkConf();
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      //JavaSparkContext ctx = new JavaSparkContext("yarn-cluster", "mytestprogram", conf); // yarn-specific
      JavaSparkContext ctx = new JavaSparkContext(conf);
      return ctx;
   }


   public static void main(String[] args) throws Exception {
  
      // create a spark context object
      JavaSparkContext ctx = createJavaSparkContext();
      
      // create a List<Tuple2<String,String>>
      List<Tuple2<String,String>> listR = new ArrayList<Tuple2<String,String>>();
      listR.add(new Tuple2<String,String>("a1", "a2"));
      listR.add(new Tuple2<String,String>("b1", "b2"));
      listR.add(new Tuple2<String,String>("c1", "c2"));

      // create a List<Tuple2<String,String>>
      List<Tuple2<String,String>> listS = new ArrayList<Tuple2<String,String>>();
      listS.add(new Tuple2<String,String>("d1", "d2"));
      listS.add(new Tuple2<String,String>("e1", "e2"));
      listS.add(new Tuple2<String,String>("f1", "f2"));
      listS.add(new Tuple2<String,String>("g1", "g2"));

      // create two RDD's
      JavaPairRDD<String,String> R = ctx.parallelizePairs(listR);
      JavaPairRDD<String,String> S = ctx.parallelizePairs(listS);

      // <U> JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)
      // Return the Cartesian product of this RDD and another one,
      // that is, the RDD of all pairs of elements (a, b) 
      // where a is in this and b is in other.
      JavaPairRDD<Tuple2<String,String>, Tuple2<String,String>> cart = R.cartesian(S);

      // save the result
      cart.saveAsTextFile("/output/z");

      // done
      ctx.close();
      System.exit(0);
   }
}

/*
# hadoop fs -cat /output/z/part*
((a1,a2),(d1,d2))
((a1,a2),(e1,e2))
((a1,a2),(f1,f2))
((a1,a2),(g1,g2))
((b1,b2),(d1,d2))
((b1,b2),(e1,e2))
((c1,c2),(d1,d2))
((c1,c2),(e1,e2))
((b1,b2),(f1,f2))
((b1,b2),(g1,g2))
((c1,c2),(f1,f2))
((c1,c2),(g1,g2))
*/