package org.dataalgorithms.chap16.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

/**
 * Maps graph edges to (Long,Long) pairs as (node1, node2).
 *
 * @author Mahmoud Parsian
 *
 */ 
public class GraphEdgeMapper 
  extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
  
	// reuse objects
	LongWritable k2 = new LongWritable();
	LongWritable v2 = new LongWritable();
	
	/**
	 * @param key is generated, ignored 
	 * @param value = "<nodeA><whitespace><nodeB>
	 */
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String edge = value.toString().trim();
		String[] nodes = StringUtils.split(edge, " ");
		long nodeA = Long.parseLong(nodes[0]);
		long nodeB = Long.parseLong(nodes[1]);		
		k2.set(nodeA);
		v2.set(nodeB);
		// edges are undirected, list A -> B and B -> A
		context.write(k2, v2);
		context.write(v2, k2);
	}
}

