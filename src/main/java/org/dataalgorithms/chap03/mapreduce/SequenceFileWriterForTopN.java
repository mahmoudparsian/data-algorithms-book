package org.dataalgorithms.chap03.mapreduce;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import java.util.Random;

/**
 * This is a driver class, which creates a sample 
 * SequenceFile of (K: Text, V: Integer) pairs.
 *
 * This is for demo/testing purposes.
 *
 * @author Mahmoud Parsian
 *
 */
public class SequenceFileWriterForTopN {

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IOException("usage: java org.dataalgorithms.chap03.mapreduce.SequenceFileWriterForTopN <hdfs-path> <number-of-entries>");
        }
        //
        Random randomNumberGenerator = new Random();
        //
        final String uri = args[0];                 // HDFS path like: /topn/input/sample.seq
        final int N = Integer.parseInt(args[1]);    // number of entries in the sequnece file, for example 20
        //
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        //
        Text key = new Text();
        IntWritable value = new IntWritable();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
            for (int i = 1; i < N; i++) {
                int randomInt = randomNumberGenerator.nextInt(100);
                key.set("cat" + i);
                value.set(randomInt);
                System.out.printf("%s\t%s\n", key, value);
                writer.append(key, value);
            }
        } 
        finally {
            IOUtils.closeStream(writer);
        }
    }
}
