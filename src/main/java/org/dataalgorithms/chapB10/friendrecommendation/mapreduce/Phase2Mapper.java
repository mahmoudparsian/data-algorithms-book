package org.dataalgorithms.chapB10.friendrecommendation.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Phase2Mapper: generate possible friends recommendations
 *
 * @author Mahmoud Parsian
 *
 */
public class Phase2Mapper
    extends Mapper<PairOfLongs, LongWritable, LongWritable, PairOfLongs> {
    
    @Override
    public void map(PairOfLongs key, 
                    LongWritable value, 
                    Context context) 
        throws IOException, InterruptedException {
        // identify two users with mutual number of friends
        long user1 = key.getLeft();
        long user2 = key.getRight();
        long numberOfMutualFriends = value.get();

        // s1 is a value representing that user1 has mutualFriends in common with user2
        PairOfLongs s1 = new PairOfLongs(user2, numberOfMutualFriends);
        context.write(new LongWritable(user1), s1);

        // string 2 is a value representing that user2 has mutualFriends in common with user1
        PairOfLongs s2 = new PairOfLongs(user1, numberOfMutualFriends);
        context.write(new LongWritable(user2), s2);
    }
}

