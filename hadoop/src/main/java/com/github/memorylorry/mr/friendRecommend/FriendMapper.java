package com.github.memorylorry.mr.friendRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text keyOut = new Text();
    private IntWritable valOut = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = StringUtils.split(value.toString(), ' ');
        for(int i=1; i<strs.length;i++){
            keyOut.set(combine(strs[0], strs[i]));
            valOut.set(0);
            context.write(keyOut, valOut);

            for(int j=i+1;j<strs.length;j++){
                keyOut.set(combine(strs[i], strs[j]));
                valOut.set(1);
                context.write(keyOut, valOut);
            }
        }

    }

    private static String combine(String a, String b){
        if(a.compareTo(b)>0){
            return a+":"+b;
        }else{
            return b+":"+a;
        }
    }
}
