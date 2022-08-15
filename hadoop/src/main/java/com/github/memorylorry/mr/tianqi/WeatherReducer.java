package com.github.memorylorry.mr.tianqi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Weather, IntWritable, Text, IntWritable> {
    Text keyOut = new Text();
    IntWritable valOut = new IntWritable();

    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        keyOut.set(key.toString());
//        for(IntWritable v:values){
//            valOut.set(v.get());
//            break;
//        }
//
//        context.write(keyOut, valOut);
        int day = 0;
        int flag = 0;
        for(IntWritable v:values){
            if(flag == 0){
                keyOut.set(key.toString());
                valOut.set(key.getTemperature());
                flag++;
                day = key.getDay();
                context.write(keyOut, valOut);
            }
            if(flag != 0 && day!=key.getDay()){
                keyOut.set(key.toString());
                valOut.set(key.getTemperature());
                context.write(keyOut, valOut);
                break;
            }
        }
    }
}
