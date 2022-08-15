package com.github.memorylorry.mr.tianqi;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSortComparator extends WritableComparator {
    public WeatherSortComparator(){
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather tq1 = (Weather) a;
        Weather tq2 = (Weather) b;
        return tq1.compareTo(tq2);
    }

}
