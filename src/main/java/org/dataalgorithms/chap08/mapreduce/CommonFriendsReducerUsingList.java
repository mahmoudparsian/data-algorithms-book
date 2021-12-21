package org.dataalgorithms.chap08.mapreduce;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import edu.umd.cloud9.io.array.ArrayListOfLongsWritable;

/**
 * A reducer class that just emits the common friends 
 *
 * @author Mahmoud Parsian
 *
 */
public class CommonFriendsReducerUsingList
        extends Reducer<Text, ArrayListOfLongsWritable, Text, Text> {

    /**
     * The goal is to find common friends by intersecting all lists defined in values parameter.
     *
     * @param key is a pair: <user_id_1><,><user_id_2>
     * @param values is a list of { <friend_1><,>...<,><friend_n> }
     */
    @Override
    public void reduce(Text key, Iterable<ArrayListOfLongsWritable> values, Context context)
            throws IOException, InterruptedException {
        // map<k, v> where k is userID, and v is the count
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        Iterator<ArrayListOfLongsWritable> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()) {
            ArrayListOfLongsWritable friends = iterator.next();
            if (friends == null) {
                context.write(key, null);
                return;
            }
            addFriends(map, friends);
            numOfValues++;
        }

        // now iterate the map to see how many have numOfValues
        List<Long> commonFriends = new ArrayList<Long>();
        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            //System.out.println(entry.getKey() + "/" + entry.getValue());
            if (entry.getValue() == numOfValues) {
                commonFriends.add(entry.getKey());
            }
        }

        // sen it to output
        context.write(key, new Text(commonFriends.toString()));
    }

    static void addFriends(Map<Long, Integer> map, ArrayListOfLongsWritable friendsList) {
        Iterator<Long> iterator = friendsList.iterator();
        while (iterator.hasNext()) {
            long id = iterator.next();
            Integer count = map.get(id);
            if (count == null) {
                map.put(id, 1);
            } else {
                map.put(id, ++count);
            }
        }
    }

}
