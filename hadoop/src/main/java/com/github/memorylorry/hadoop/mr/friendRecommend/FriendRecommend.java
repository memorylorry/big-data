package com.github.memorylorry.hadoop.mr.friendRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendRecommend {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
        Job job= Job.getInstance(conf);
        job.setJobName("FriendRecommend");
        job.setJarByClass(FriendRecommend.class);

        // MAP
        job.setMapperClass(FriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // REDUCE
        job.setReducerClass(FriendReducer.class);

        Path input = new Path("/data/friend");
        FileInputFormat.addInputPath(job, input);
        Path output = new Path("/f_out");
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
