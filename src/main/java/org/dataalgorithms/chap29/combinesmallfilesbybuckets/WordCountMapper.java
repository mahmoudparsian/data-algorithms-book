package org.dataalgorithms.chap29.combinesmallfilesbybuckets;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

/**
 * Word Count Mapper
 *
 * For each line of input, break the line into words
 * and emit them as (<b>word</b>, <b>1</b>).
 *
 * @author Mahmoud Parsian
 *
 */

public class WordCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {

 	private int ignoredLength = 3; // default
	private static final IntWritable one = new IntWritable(1);
	private Text reducerKey = new Text();

	protected void setup(Context context)
    		throws IOException,InterruptedException {
		this.ignoredLength = context.getConfiguration().getInt("word.count.ignored.length", 3);
	}

	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		
	  	String line = value.toString().trim();	  	
	  	if ((line == null) || (line.length() < ignoredLength)) {
	  		return;
	  	}
	  	
        String[] words = StringUtils.split(line);
        if (words == null) {
        	return;
        }
        
	  	for (String word : words) {
	  	    if (word.length() < this.ignoredLength) {
	  	    	// ignore strings with less than size 3
	  	    	continue;
	  	    }
	  	    if (word.matches(".*[,.;]$")) {
	  	        // remove the special char from the end
	  	    	word = word.substring(0, word.length() -1); 
	  	    }
			reducerKey.set(word);
			context.write(reducerKey, one);
	 	}
	}
	
}
