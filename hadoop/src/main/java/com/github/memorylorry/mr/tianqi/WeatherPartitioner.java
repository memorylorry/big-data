package com.github.memorylorry.mr.tianqi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<Weather, IntWritable> {

    @Override
    public int getPartition(Weather weather, IntWritable intWritable, int numPartitions) {
        return weather.getYear() % numPartitions;
    }
}
