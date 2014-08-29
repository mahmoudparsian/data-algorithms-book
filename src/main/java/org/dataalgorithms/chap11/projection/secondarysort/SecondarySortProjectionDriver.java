package org.dataalgorithms.chap11.projection.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import edu.umd.cloud9.io.pair.PairOfLongInt;
import org.apache.log4j.Logger;

import org.dataalgorithms.util.HadoopUtil;

/**
  * MapReduce job for projecting customer transaction data
  * by using MapReduce's "secondary sort" (sort by shuffle function).
  * Note that reducer values arrive sorted by implementing the "secondary sort"
  * design pattern (no data is sorted in memory).
  * 
  * @author Mahmoud Parsian
  *
  */
public class SecondarySortProjectionDriver {

	private static final Logger theLogger = 
	   Logger.getLogger(SecondarySortProjectionDriver.class); 
 
    public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();	
        Configuration conf = new Configuration();
		JobConf jobconf = new JobConf(conf, SecondarySortProjectionDriver.class);
		jobconf.setJobName("SecondarySortProjectionDriver");
    
       String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (otherArgs.length != 2) {
          System.err.println("Usage: SecondarySortProjectionDriver  <input> <output>");
          System.exit(1);
       }

       // add jars to distributed cache
       HadoopUtil.addJarsToDistributedCache(conf, "/lib/");
       
       // set mapper/reducer
       jobconf.setMapperClass(SecondarySortProjectionMapper.class);
       jobconf.setReducerClass(SecondarySortProjectionReducer.class);
       
       // define mapper's output key-value
       jobconf.setMapOutputKeyClass(CompositeKey.class);
       jobconf.setMapOutputValueClass(PairOfLongInt.class);
              
       // define reducer's output key-value
       jobconf.setOutputKeyClass(Text.class);
       jobconf.setOutputValueClass(Text.class);
       
       // define I/O
	   FileInputFormat.setInputPaths(jobconf, new Path(otherArgs[0]));
	   FileOutputFormat.setOutputPath(jobconf, new Path(otherArgs[1]));
       
       jobconf.setInputFormat(TextInputFormat.class); 
       jobconf.setOutputFormat(TextOutputFormat.class);
	   jobconf.setCompressMapOutput(true);       
       
       // the following 3 setting are needed for "secondary sorting"
       // Partitioner decides which mapper output goes to which reducer 
       // based on mapper output key. In general, different key is in 
       // different group (Iterator at the reducer side). But sometimes, 
       // we want different key in the same group. This is the time for 
       // Output Value Grouping Comparator, which is used to group mapper 
       // output (similar to group by condition in SQL).  The Output Key 
       // Comparator is used during sort stage for the mapper output key.
       jobconf.setPartitionerClass(NaturalKeyPartitioner.class);
       jobconf.setOutputKeyComparatorClass(CompositeKeyComparator.class);
       jobconf.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);
       
       JobClient.runJob(jobconf).waitForCompletion();
       
	   long elapsedTime = System.currentTimeMillis() - startTime;
       theLogger.info("elapsedTime (in milliseconds): "+ elapsedTime);      
       System.exit(0);
       
    }

}





