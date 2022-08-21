package com.github.memorylorry.hadoop.mr.friendRecommend2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FriendMapper2 extends Mapper<LongWritable, Text, Friend, IntWritable> {
    private IntWritable valOut = new IntWritable();
    Friend friend = new Friend();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // hadoop:cat	2
        String[] strs = StringUtils.split(value.toString(), '\t');
        String[] names = StringUtils.split(strs[0], ':');

        valOut.set(Integer.parseInt(strs[1]));
        friend.setWeight(Integer.parseInt(strs[1]));
        friend.setName(names[0]);
        friend.setFriend(names[1]);
        context.write(friend, valOut);

        friend.setName(names[1]);
        friend.setFriend(names[0]);
        context.write(friend, valOut);
    }
}
