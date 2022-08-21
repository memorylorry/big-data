package com.github.memorylorry.hadoop.mr.friendRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable valOut = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //    tom:hello 0
        //    hello:hadoop 1
        //    hadoop:cat 1
        int sum = 0;
        for(IntWritable v:values){
            sum += v.get();
        }
        if(sum >0) {
            valOut.set(sum);
            context.write(key, valOut);
        }
    }
}
