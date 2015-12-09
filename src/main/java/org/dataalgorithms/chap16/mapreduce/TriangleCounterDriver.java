package org.dataalgorithms.chap16.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.umd.cloud9.io.pair.PairOfLongs;

/**
 *
 * Driver program, which submits 3 MapReduce jobs: job1, job2, and job3.
 *
 * Takes two arguments, the edges file and output file.
 *
 * Input File must be of the form:
 *
 * <node1><space><node2>
 * where node1: long node2: long
 *
 * @author Mahmoud Parsian
 *
 */
public class TriangleCounterDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        //  key hadoop generated, ignored here
        //  value = <node1><space><node2>
        Job job1 = new Job(getConf());
        job1.setJobName("triads");
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(PairOfLongs.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setJarByClass(TriangleCounterDriver.class);
        job1.setMapperClass(GraphEdgeMapper.class);
        job1.setReducerClass(GraphEdgeReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/triangles/tmp1"));

        // key = PairOfLongs
        // value = Long
        Job job2 = new Job(getConf());
        job2.setJobName("triangles");
        job2.setMapOutputKeyClass(PairOfLongs.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setJarByClass(TriangleCounterDriver.class);
        job2.setMapperClass(TriadsMapper.class);
        job2.setReducerClass(TriadsReducer.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path("/triangles/tmp1"));
        FileOutputFormat.setOutputPath(job2, new Path("/triangles/tmp2"));

        Job job3 = new Job(getConf());
        job3.setJobName("count");
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setJarByClass(TriangleCounterDriver.class);
        job3.setMapperClass(UniqueTriadsMapper.class);
        job3.setReducerClass(UniqueTriadsReducer.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path("/triangles/tmp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        int status = job1.waitForCompletion(true) ? 0 : 1;
        if (status == 0) {
            status = job2.waitForCompletion(true) ? 0 : 1;
        }
        if (status == 0) {
            status = job3.waitForCompletion(true) ? 0 : 1;
        }
        return status;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCounterDriver(), args);
        System.exit(res);
    }
}
