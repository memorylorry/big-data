package com.github.memorylorry.hadoop.mr.wordcount;

import com.github.memorylorry.hadoop.mr.util.HDFSCheckAndDel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://n1:9000");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.setInt("dfs.replication", 1);
        System.setProperty("HADOOP_USER_NAME", "centos");

        HDFSCheckAndDel.check(args[1]);

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);

        // Specify various job-specific parameters
        job.setJobName("HuWordCount");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MyMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.submit();
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }

}
