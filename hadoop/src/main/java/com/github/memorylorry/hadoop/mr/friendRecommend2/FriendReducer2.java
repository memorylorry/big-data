package com.github.memorylorry.hadoop.mr.friendRecommend2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer2 extends Reducer<Friend, IntWritable, Text, IntWritable> {
    Text keyOut = new Text();
    IntWritable valOut = new IntWritable();
    @Override
    protected void reduce(Friend key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for(IntWritable v:values){
            keyOut.set(key.toString());
            valOut.set(v.get());
            context.write(keyOut, valOut);
        }
    }
}
