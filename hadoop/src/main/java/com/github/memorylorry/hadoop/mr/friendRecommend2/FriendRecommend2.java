package com.github.memorylorry.hadoop.mr.friendRecommend2;

import com.github.memorylorry.hadoop.mr.friendRecommend.FriendRecommend;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendRecommend2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
        Job job= Job.getInstance(conf);
        job.setJobName("FriendRecommend2");
        job.setJarByClass(FriendRecommend.class);

        // MAP
        job.setMapperClass(FriendMapper2.class);
        job.setMapOutputKeyClass(Friend.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(FriendSortComparator.class);
        // REDUCE
        job.setGroupingComparatorClass(FriendGroupingComparator.class);
        job.setReducerClass(FriendReducer2.class);

        Path input = new Path("/f_out");
        FileInputFormat.addInputPath(job, input);
        Path output = new Path("/f_out2");
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
