package com.github.memorylorry.mr.tianqi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HotWeatherMR {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "centos");

        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJobName("HotWeatherMR");
        job.setJarByClass(HotWeatherMR.class);

        // MAP
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class);
//        job.setPartitionerClass(WeatherPartitioner.class);
//        job.setSortComparatorClass(WeatherSortComparator.class);

        // MAP END

        // REDUCE
//        job.setGroupingComparatorClass(WeatherGroupingComparator.class);
        job.setReducerClass(WeatherReducer.class);
        // REDUCE END

        Path input = new Path("/data/tq");
        FileInputFormat.addInputPath(job, input);
        Path output = new Path("/tq_out");
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
