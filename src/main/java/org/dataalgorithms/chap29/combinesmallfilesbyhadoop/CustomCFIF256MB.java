package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

/**
 *  A custom file input format, which combines/merges smaller 
 *  files into big files controlled by MAX_SPLIT_SIZE_256MB
 *
 * @author Mahmoud Parsian
 *
 */
public class CustomCFIF256MB extends CustomCFIF {
   final static long MAX_SPLIT_SIZE_256MB = 268435456; // 256 MB = 256*1024*1024
   
   public CustomCFIF256MB() {
      super();
      setMaxSplitSize(MAX_SPLIT_SIZE_256MB); 
   }
}