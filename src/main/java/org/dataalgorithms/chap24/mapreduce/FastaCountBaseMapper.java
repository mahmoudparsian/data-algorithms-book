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

/**
 * The FastaCountBaseMapper class implements MapReduce's
 * map() method.
 *
 * 
 * @author Mahmoud Parsian
 *
 */
public class FastaCountBaseMapper 
  extends Mapper<Object, Text, Text, LongWritable> {
    
    private Map<Character, Long> dnaBaseCounter = null;  
    private LongWritable counter = new LongWritable();
	private Text base = new Text();
 	private static final Logger LOG = Logger.getLogger(FastaCountBaseMapper.class);

	/**
	 * Called once at the beginning of the task. 
	 */ 
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
       throws IOException, InterruptedException {
       dnaBaseCounter = new HashMap<Character, Long>();  
    }

    /**
     * Called once at the end of the task. 
     */   
    protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException {
        // now iterate the baseCounter and emit <key, value>
		for (Map.Entry<Character, Long> entry : dnaBaseCounter.entrySet()) {       
           base.set(entry.getKey().toString());
           counter.set(entry.getValue());
           context.write(base, counter);      
        }
    }
    
    public void map(Object key, Text value, Context context) 
       throws IOException, InterruptedException {
	   String fasta = value.toString();
	   String[] lines = fasta.split("[\\r\\n]+");
	   for(int j=1; j<lines.length; j++){
		  char[] array = lines[j].toCharArray();
		  for(char c : array){
			  Long v = dnaBaseCounter.get(c);
			  if (v == null) {
			     dnaBaseCounter.put(c, 1l);
			  }
			  else {
			     v++;
			     dnaBaseCounter.put(c, v);
			  }
		  }
	   }

    }
}

