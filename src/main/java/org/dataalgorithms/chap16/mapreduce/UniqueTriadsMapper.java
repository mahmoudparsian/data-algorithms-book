package org.dataalgorithms.chap16.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;
import java.util.Arrays;

/**
 * Generate unique triangles
 *
 * @author Mahmoud Parsian
 *
 */ 
public class UniqueTriadsMapper 
  extends Mapper<Text, Text, Text, Text> {
  
    static Text sortedKey = new Text();
    
  	// key = "<nodeA><,><nodeB><,><nodeC>
	// value = ""
	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
		// sorted(x, y, z) = sort(<nodeA><,><nodeB><,><nodeC>)
		// x < y < z
		String[] nodes = StringUtils.split(key.toString(), ","); 
		Arrays.sort(nodes);
		sortedKey.set(nodes[0]+","+nodes[1]+","+nodes[2]);
		context.write(sortedKey, value);
	}
	
}
