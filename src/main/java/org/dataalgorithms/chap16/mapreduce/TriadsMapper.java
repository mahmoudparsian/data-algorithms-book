package org.dataalgorithms.chap16.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import edu.umd.cloud9.io.pair.PairOfLongs;

/**
 * Identity mapper.
 *
 * @author Mahmoud Parsian
 *
 */ 
public class TriadsMapper 
  extends Mapper<PairOfLongs, LongWritable, PairOfLongs, LongWritable> {

	public void map(PairOfLongs key, LongWritable value, Context context)
		throws IOException, InterruptedException {
			context.write(key, value);
	}
}

