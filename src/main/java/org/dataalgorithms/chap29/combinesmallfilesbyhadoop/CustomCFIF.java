package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;

import edu.umd.cloud9.io.pair.PairOfStringLong;
// PairOfStringLong = Tuple2<String, Long> = Tuple2<FileName, Offset>
// https://raw.githubusercontent.com/lintool/Cloud9/master/src/dist/edu/umd/cloud9/io/pair/PairOfStringLong.java

/**
 *  A custom file input format, which combines/merges smaller 
 *  files into big files controlled by MAX_SPLIT_SIZE_64MB
 *
 * @author Mahmoud Parsian
 *
 */
public class CustomCFIF extends CombineFileInputFormat<PairOfStringLong, Text> {
   final static long MAX_SPLIT_SIZE_64MB = 67108864; // 64 MB = 64*1024*1024
   
   public CustomCFIF() {
      super();
      setMaxSplitSize(MAX_SPLIT_SIZE_64MB); 
   }
  
   public RecordReader<PairOfStringLong, Text> createRecordReader(InputSplit split, 
                                                                  TaskAttemptContext context) 
      throws IOException {
      return new CombineFileRecordReader<PairOfStringLong, Text>((CombineFileSplit)split, 
                                                                 context, 
                                                                 CustomRecordReader.class);
   }
  
   @Override
   protected boolean isSplitable(JobContext context, Path file) {
      return false;
   }
}