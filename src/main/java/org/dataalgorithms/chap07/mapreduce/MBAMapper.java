package org.dataalgorithms.chap07.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;
import org.apache.commons.lang3.StringUtils;

import org.dataalgorithms.util.Combination;


/**
 * Implement the map() function.
 *
 * Market Basket Analysis Algorithm: find the association rule for the list of items 
 * in a basket; That is, there are transaction data in a store
 * <ul>
 * <li>trx1: apple, cracker, soda, corn </li>
 * <l1>trax2: icecream, soda, bread</li>
 * <li>...</li>
 * <ul>
 * 
 * <p>
 * The code reads the data as 
 * key: first item
 * value: the rest of the items
 *
 * @author Mahmoud Parsian
 *
 */
public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

   public static final Logger THE_LOGGER = Logger.getLogger(MBAMapper.class);

   public static final int DEFAULT_NUMBER_OF_PAIRS = 2; 

   //output key2: list of items paired; can be 2 or 3 ...
   private static final Text reducerKey = new Text(); 
   
   //output value2: number of the paired items in the item list
   private static final IntWritable NUMBER_ONE = new IntWritable(1);

   int numberOfPairs; // will be read by setup(), set by driver
   
   @Override
   protected void setup(Context context) throws IOException, InterruptedException {
      this.numberOfPairs = context.getConfiguration().getInt("number.of.pairs", DEFAULT_NUMBER_OF_PAIRS);
      THE_LOGGER.info("setup() numberOfPairs = " + numberOfPairs);
    }

   @Override
   public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {

      // input line
      String line = value.toString();
      List<String> items = convertItemsToList(line);
      if ((items == null) || (items.isEmpty())) {
         // no mapper output will be generated
         return;
      }
      generateMapperOutput(numberOfPairs, items, context);
   }
   
   private static List<String> convertItemsToList(String line) {
      if ((line == null) || (line.length() == 0)) {
         // no mapper output will be generated
         return null;
      }      
      String[] tokens = StringUtils.split(line, ",");   
      if ( (tokens == null) || (tokens.length == 0) ) {
         return null;
      }
      List<String> items = new ArrayList<String>();         
      for (String token : tokens) {
         if (token != null) {
             items.add(token.trim());
         }         
      }         
      return items;
   }
   
   /**
    * 
    * build <key, value> by sorting the input list
    * If not sort the input, it may have duplicated list but not considered as a same list.
    * ex: (a, b, c) and (a, c, b) might become different items to be counted if not sorted
    * @param numberOfPairs   number of pairs associated
    * @param items      list of items (from input line)
    * @param context   Hadoop Job context
    * @throws IOException
    * @throws InterruptedException
    */
   private void generateMapperOutput(int numberOfPairs, List<String> items, Context context) 
      throws IOException, InterruptedException {
      List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
      for (List<String> itemList: sortedCombinations) {
         System.out.println("itemlist="+itemList.toString());
         reducerKey.set(itemList.toString());
         context.write(reducerKey, NUMBER_ONE);
      }   
   }
   
}

