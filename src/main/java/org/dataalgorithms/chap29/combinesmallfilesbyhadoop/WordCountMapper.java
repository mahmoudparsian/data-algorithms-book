package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

import edu.umd.cloud9.io.pair.PairOfStringLong;
// PairOfStringLong = Tuple2<String, Long> = Tuple2<FileName, Offset>

/**
 * Counts the words in each line.
 * For each line of input, break the line into words 
 * and emit them as (<b>word</b>, <b>1</b>).
 *
 * @author Mahmoud Parsian
 */
public class WordCountMapper extends 
	 Mapper<PairOfStringLong, Text, Text, IntWritable> {

    final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(PairOfStringLong key, 
                    Text value,
                    Context context) 
       throws IOException, InterruptedException {
       String line = value.toString().trim();
       String[] tokens = StringUtils.split(line, " ");
       for (String tok : tokens) {
          word.set(tok);
          context.write(word, one);  
       }
    }
}
