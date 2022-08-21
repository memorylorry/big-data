package com.github.memorylorry.hadoop.mr.friendRecommend2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FriendGroupingComparator extends WritableComparator {
    public FriendGroupingComparator(){
        super(Friend.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Friend w1 = (Friend) a;
        Friend w2 = (Friend) b;
        return w1.getName().compareTo(w2.getName());
    }
}
