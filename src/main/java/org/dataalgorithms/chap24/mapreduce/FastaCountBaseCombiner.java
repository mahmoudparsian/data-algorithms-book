package org.dataalgorithms.chap24.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The FastaCountBaseCombiner class implements MapReduce's
 * combine() method (in Hadoop, we call it reduce() method).
 * Note that input for combine() and reduce()  must corespond to 
 * the output of mappers.

 * This class implements the combine() function for counting 
 * DBNA bases.
 * 
 * @author Mahmoud Parsian
 *
 */
public class FastaCountBaseCombiner
  extends Reducer<Text, LongWritable, Text, LongWritable> {
  
    public void reduce(Text key, Iterable<LongWritable> values,  Context context) 
       throws IOException, InterruptedException {
	   long total = 0;
	   for(LongWritable val: values){
		  total += val.get();
	   }
	   context.write(key,new LongWritable(total));
    }
}





