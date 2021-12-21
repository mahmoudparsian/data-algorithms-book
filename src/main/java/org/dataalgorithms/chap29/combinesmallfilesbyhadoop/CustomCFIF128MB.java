package org.dataalgorithms.chap29.combinesmallfilesbyhadoop;

/**
 *  A custom file input format, which combines/merges smaller 
 *  files into big files controlled by MAX_SPLIT_SIZE_128MB
 *
 * @author Mahmoud Parsian
 *
 */
public class CustomCFIF128MB extends CustomCFIF {
   final static long MAX_SPLIT_SIZE_128MB = 134217728; // 128 MB  = 128*1024*1024
   
   public CustomCFIF128MB() {
      super();
      setMaxSplitSize(MAX_SPLIT_SIZE_128MB); 
   }
}