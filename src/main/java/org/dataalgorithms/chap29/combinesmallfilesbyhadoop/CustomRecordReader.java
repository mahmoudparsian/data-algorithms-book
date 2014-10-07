package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import edu.umd.cloud9.io.pair.PairOfStringLong;
// PairOfStringLong = Tuple2<String, Long> = Tuple2<FileName, Offset>

/**
 * A custom record reader class.
 *
 * @author Mahmoud Parsian
 *
 */
public class CustomRecordReader extends RecordReader<PairOfStringLong, Text> {
   // define (K,V)
   private PairOfStringLong key; 
   private Text value;
   
   // define pos and offsets
   private long startOffset;
   private long endOffset;
   private long pos;
   
   private FileSystem fs;
   private Path path;
   private FSDataInputStream fileIn;
   private LineReader reader;

   public CustomRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) 
      throws IOException{
      path = split.getPath(index);
      fs = path.getFileSystem(context.getConfiguration());
      startOffset = split.getOffset(index);
      endOffset = startOffset + split.getLength(index);
      fileIn = fs.open(path);
      reader = new LineReader(fileIn);
      pos = startOffset;
   }

   @Override
   public void initialize(InputSplit arg0, TaskAttemptContext arg1)
      throws IOException, InterruptedException {
      // This will not be called, use custom Constructor
   }

   @Override
   public void close() throws IOException {
   }

   @Override
   public float getProgress() throws IOException {
      if (startOffset == endOffset) {
         return 0;
      }
      return Math.min(1.0f, (pos - startOffset) / (float) (endOffset - startOffset));
   }

   @Override
   public PairOfStringLong getCurrentKey() throws IOException, InterruptedException {
      return key;
   }

   @Override
   public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
   }

   @Override
   public boolean nextKeyValue() throws IOException {
      if (key == null) {
         // key.filename = path.getName()
         // key.offset = pos
         key = new PairOfStringLong(path.getName(), pos);
      }
      if (value == null){
         value = new Text();
      }
      int newSize = 0;
      if (pos < endOffset) {
         newSize = reader.readLine(value);
         pos += newSize;
      }
      if (newSize == 0) {
         key = null;
         value = null;
         return false;
      } 
      else{
         return true;
      }
   }
}
