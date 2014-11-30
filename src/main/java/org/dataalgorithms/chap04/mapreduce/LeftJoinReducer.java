package org.dataalgorithms.chap04.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.umd.cloud9.io.pair.PairOfStrings;
import java.util.Iterator;

/** 
 * LeftJoinReducer implements the reduce() function for 
 * the "left join" design pattern.
 *
 *
 *  values = List <Pair1, Pair2, Pair3, ...>, where the 
 *  first pair (Pair1) is location; if it's not, then we 
 *  don't have a user record, so we'll set the locationID 
 *  as "undefined"
 *  
 * @author Mahmoud Parsian
 *
 */
public class LeftJoinReducer 
	extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

   Text productID = new Text();
   Text locationID = new Text("undefined");
   
   @Override
   public void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) 
      throws java.io.IOException, InterruptedException {
      
      Iterator<PairOfStrings> iterator = values.iterator();
      if (iterator.hasNext()) {
      	 // firstPair must be location pair
      	 PairOfStrings firstPair = iterator.next(); 
      	 System.out.println("firstPair="+firstPair.toString());
         if (firstPair.getLeftElement().equals("L")) {
            locationID.set(firstPair.getRightElement());
         }
      } 	 
      	       	 
      while (iterator.hasNext()) {
      	 // the remaining elements must be product pair
      	 PairOfStrings productPair = iterator.next(); 
      	 System.out.println("productPair="+productPair.toString());
         productID.set(productPair.getRightElement());
         context.write(productID, locationID);
      }
   }

}
