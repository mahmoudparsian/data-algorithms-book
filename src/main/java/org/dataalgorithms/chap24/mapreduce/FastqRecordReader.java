package org.dataalgorithms.chap24.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * This class define a custom RecordReader for FASTQ files for the 
 * Hadoop MapReduce framework.
 
 * @author Mahmoud Parsian
 */
public class FastqRecordReader extends RecordReader<LongWritable, Text> {

  // input data comes from lrr
  private LineRecordReader lrr = null; 

  private LongWritable key = new LongWritable();
  private Text value = new Text();

  private final String[] lines = new String[4];
  private final long[] keys = new long[4];


  @Override
  public void close() throws IOException {
    this.lrr.close();
  }

  @Override
  public LongWritable getCurrentKey() 
    throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() 
     throws IOException, InterruptedException {
     return value;
  }

  @Override
  public float getProgress() 
     throws IOException, InterruptedException {
     return this.lrr.getProgress();
  }

  @Override
  public void initialize(final InputSplit inputSplit,
      					 final TaskAttemptContext taskAttemptContext) 
      throws IOException, InterruptedException {
      this.lrr = new LineRecordReader();
      this.lrr.initialize(inputSplit, taskAttemptContext);
  }
  
  @Override
  public boolean nextKeyValue() 
    throws IOException, InterruptedException {
    int count = 0;
    boolean found = false;

    while (!found) {

      if (!this.lrr.nextKeyValue()) {
        return false;
      }

      final String s = this.lrr.getCurrentValue().toString().trim();
	  System.out.println("nextKeyValue() s="+s);
	  
      // Prevent empty lines
      if (s.length() == 0) {
        continue;
      }

      this.lines[count] = s;
      this.keys[count] = this.lrr.getCurrentKey().get();

      if (count < 3) {
        count++;
      }
      else {
         if (this.lines[0].charAt(0) == '@' && this.lines[2].charAt(0) == '+') {
           found = true;
         }
         else {
           shiftLines(); 
           shiftPositions(); //this.keys[i] = this.keys[i+1];
         }
      }

    } //end-while

    // set key
    this.key = new LongWritable(this.keys[0]);    
    // set value
    this.value = buildValue();
    // clear records for next FASTQ sequence
    clearRecords();

    return true;
  }

  private void shiftLines() {
    // this.lines[i] = this.lines[i+1];
    this.lines[0] = this.lines[1];
    this.lines[1] = this.lines[2];
    this.lines[2] = this.lines[3];
  }
          
  private void shiftPositions() { 
    //this.keys[i] = this.keys[i+1];
    this.keys[0] = this.keys[1];
    this.keys[1] = this.keys[2];
    this.keys[2] = this.keys[3];
  }
  
  private void clearRecords() {
    lines[0] = null;
    lines[1] = null;
    lines[2] = null;
    lines[3] = null;
  }
  
  private Text buildValue() {
	StringBuilder builder = new StringBuilder();
	builder.append(lines[0]);
	builder.append(",;,");
	builder.append(lines[1]);
	builder.append(",;,");
	builder.append(lines[2]);
	builder.append(",;,");
	builder.append(lines[3]);
	return new Text(builder.toString());	
  }  
}
