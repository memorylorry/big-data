package com.github.memorylorry.mr.tianqi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroupingComparator extends WritableComparator {
    public WeatherGroupingComparator(){
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;

        int c1 = Integer.compare(w1.getYear(), w2.getYear());
        if(c1 == 0){
            int c2 = Integer.compare(w1.getMonth(), w2.getMonth());
            return c2;
        }

        return c1;
    }
}
