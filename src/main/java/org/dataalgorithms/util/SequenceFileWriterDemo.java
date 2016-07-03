package org.dataalgorithms.util;

import java.io.IOException;
import java.net.URI;
//
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

/**
 * This is a driver class, which creates a SequenceFile 
 * of (K: Text, V: Long) pairs (for demo/testing purposes).
 *
 * @author Mahmoud Parsian
 *
 */
public class SequenceFileWriterDemo {
  
   public static void main(String[] args) throws IOException {
      String uri = args[0];
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(URI.create(uri), conf);
      Path path = new Path(uri);

      Text key = new Text();
      LongWritable value = new LongWritable();
      SequenceFile.Writer writer = null;
      try {
         writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass()); 
         for (long i = 1; i < 6; i++) {
            for (long j = 1; j < 6; j++) {
                key.set("key"+i);
                value.set(i*j);
                System.out.printf("%s\t%s\n", key, value);
                writer.append(key, value);
            }
         }
      } 
      finally {
         IOUtils.closeStream(writer);
      }
   }
}
