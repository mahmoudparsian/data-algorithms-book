package org.dataalgorithms.chap24.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.dataalgorithms.util.InputOutputUtil;


@InterfaceAudience.Public
@InterfaceStability.Stable
public class FastaInputFormat extends FileInputFormat<LongWritable, Text> { 
  public static final String LINES_PER_MAP = 
     "mapreduce.input.lineinputformat.linespermap";

  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit genericSplit, TaskAttemptContext context) 
      throws IOException {
     context.setStatus(genericSplit.toString());
     return new FastaRecordReader();
  }

  /** 
   * Logically splits the set of input files for the job, splits N lines
   * of the input as one split.
   * 
   * @see FileInputFormat#getSplits(JobContext)
   */
  public List<InputSplit> getSplits(JobContext job)
  throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    int numberOfLinesPerSplit = getNumberOfLinesPerSplit(job);
    for (FileStatus status : listStatus(job)) {
		splits.addAll(getSplitsForFile(status,
									   job.getConfiguration(), 
									   numberOfLinesPerSplit)
		);
    }
    return splits;
  }
  
  public static List<FileSplit> getSplitsForFile(
		FileStatus status,
		Configuration conf, 
		int numberOfLinesPerSplit) throws IOException {
    List<FileSplit> splits = new ArrayList<FileSplit> ();
    Path fileName = status.getPath();
    if (status.isDir()) {
      throw new IOException("Not a file: " + fileName);
    }
    FileSystem  fs = fileName.getFileSystem(conf);
    LineReader lr = null;
    try {
      FSDataInputStream in  = fs.open(fileName);
      lr = new LineReader(in, conf);
      Text line = new Text();
      long begin = 0;
      long length = 0;
      int num;
	  long recordLength = 0;
	  int recordsRead = 0;
	  while ((num = lr.readLine(line)) > 0) {
		if (line.toString().indexOf(">") >= 0){
			recordsRead++;
		}
		if (recordsRead > numberOfLinesPerSplit){
			splits.add(new FileSplit(fileName, begin, recordLength, new String[]{}));
			begin = length;
			recordLength = 0;
			recordsRead = 1;
		}
				
		length += num;
		recordLength += num;
	  }
	  splits.add(new FileSplit(fileName, begin, recordLength, new String[]{}));
	} 
	finally {
		InputOutputUtil.close(lr);
	}
	return splits;
  }
  
  /**
   * Set the number of lines per split
   *
   * @param job the job to modify
   * @param numberOfLines the number of lines per split
   */
  public static void setNumberOfLinesPerSplit(Job job, int numberOfLines) {
    job.getConfiguration().setInt(LINES_PER_MAP, numberOfLines);
  }

  /**
   * Get the number of lines per split
   * @param job the job
   * @return the number of lines per split
   */
  public static int getNumberOfLinesPerSplit(JobContext job) {
    return job.getConfiguration().getInt(LINES_PER_MAP, 1);
  }
}

