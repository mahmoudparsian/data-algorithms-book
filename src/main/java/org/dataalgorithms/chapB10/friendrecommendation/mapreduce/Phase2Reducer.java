package org.dataalgorithms.chapB10.friendrecommendation.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


/**
 * A reducer class that aggregates possible friends recommendations
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class Phase2Reducer 
    extends Reducer<LongWritable, PairOfLongs, LongWritable, Text> {

    private int numberOfRecommendations = 5; // default

    // called once at the beginning of the task.
    @Override
    protected void setup(Context context)
       throws IOException,InterruptedException {
       this.numberOfRecommendations = 
          context.getConfiguration().getInt("number.of.recommendations", 5);
    }


    @Override
    public void reduce(LongWritable userAsKey, 
                       Iterable<PairOfLongs> values, 
                       Context context) 
        throws IOException, InterruptedException {
        // step-1: build sorted map of friendship
        TreeMap<Long, List<Long>> sortedMap = buildSortedMap(values);

        // step-2: now select the top numberOfRecommendations users 
        // to recommend as potential friends
        List<Long> recommendations = getTopFriends(sortedMap, numberOfRecommendations);
        if (recommendations.isEmpty()) {
           return;
        }

        // step-3: emit recommendations for a user
        StringBuilder builder = new StringBuilder();
        int counter = 0;
        for (Long recommendation : recommendations) {
           builder.append(recommendation);
           if (counter < (recommendations.size()-1)) {
              builder.append(",");
           }
           counter++;
        }
        context.write(userAsKey, new Text(builder.toString()));
    }
    
    
    static TreeMap<Long, List<Long>> buildSortedMap(Iterable<PairOfLongs> values) {
        TreeMap<Long, List<Long>> sortedMap = new TreeMap<Long, List<Long>>();
        for (PairOfLongs pair : values) {
           long potentialFriend = pair.getLeft();
           long mutualFriend = pair.getRight();
           List<Long> list = sortedMap.get(mutualFriend);
           if (list == null) {
              // there are no users yet with this number of mutual friends
              list = new ArrayList<Long>();
              list.add(potentialFriend);
              sortedMap.put(mutualFriend, list);
           }
           else {
              // there are already users with this number of mutual friends
              list.add(potentialFriend);
           }
        }
        return sortedMap;
    }
    
    
    static List<Long> getTopFriends(TreeMap<Long, List<Long>> sortedMap, int N) {   
        // now select the top N users to recommend as potential friends
        List<Long> recommendations = new ArrayList<Long>();
        for (long mutualFriends : sortedMap.descendingKeySet()) {
            List<Long> potentialFriends = sortedMap.get(mutualFriends);
            Collections.sort(potentialFriends);
            for (long potentialFriend : potentialFriends) {
                recommendations.add(potentialFriend);
                if (recommendations.size() == N) {
                   return recommendations;
                }
            }
        }
        
        // here we have less than N friends recommendations
        return recommendations;
    }    
}

