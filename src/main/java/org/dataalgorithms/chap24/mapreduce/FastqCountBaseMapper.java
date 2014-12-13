package org.dataalgorithms.chap24.mapreduce;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class FastqCountBaseMapper 
  extends Mapper<Object, Text, Text, LongWritable> {
    
    private Map<Character, Long> dnaBaseCounter = null;  
    private LongWritable counter = new LongWritable();
	private Text base = new Text();
 	private static final Logger LOG = Logger.getLogger(FastqCountBaseMapper.class);

	// Called once at the beginning of the task.  
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
       throws IOException, InterruptedException {
       dnaBaseCounter = new HashMap<Character, Long>();  
    }

    // Called once at the end of the task.    
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException {
        // now iterate the baseCounter and emit <key, value>
		for (Map.Entry<Character, Long> entry : dnaBaseCounter.entrySet()) {       
           base.set(entry.getKey().toString());
           counter.set(entry.getValue());
           context.write(base, counter);      
        }
    }
    
    public void debug(String[] tokens) {
	   LOG.info("debug(): tokens.length="+tokens.length);
	   for(int i=0; i < tokens.length; i++){
	   		LOG.info("debug(): i="+i+ "\t tokens[i]="+tokens[i]);
       }
    }
    
    public void map(Object key, Text value, Context context) 
       throws IOException, InterruptedException {
       // fastqRecord = 4 lines and lines are separated by delimiter ",;,"
	   String fastqRecord = value.toString();
	   // LOG.info("map(): fastqRecord="+fastqRecord);
	   String[] lines = fastqRecord.split(",;,");
	   debug(lines);
	   // 2nd line (lines[1]) is the sequence
	   String sequence = lines[1].toLowerCase();
	   char[] array = sequence.toCharArray(); 
	   for(char c : array){
	      Long v = dnaBaseCounter.get(c);
	      if (v == null) {
		   dnaBaseCounter.put(c, 1l);
	      }
	      else {
		   dnaBaseCounter.put(c, v+1l);
	      }
	   }
    }
}
