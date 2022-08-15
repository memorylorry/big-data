package com.github.memorylorry.mr.tianqi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeatherMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {
    private Weather weather = new Weather();
    private IntWritable temp = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = StringUtils.split(value.toString(), '\t');

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = sdf.parse(strs[0]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        weather.setYear(cal.get(Calendar.YEAR));
        weather.setMonth(cal.get(Calendar.MONTH)+1);
        weather.setDay(cal.get(Calendar.DAY_OF_MONTH));

        int tempVal = Integer.parseInt(strs[1].substring(0, strs[1].length()-1));
        weather.setTemperature(tempVal);
        temp.set(tempVal);

        context.write(weather, temp);
    }
}
