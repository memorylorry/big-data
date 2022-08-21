package com.github.memorylorry.hadoop.mr.friendRecommend2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FriendSortComparator extends WritableComparator {

    public FriendSortComparator(){
        super(Friend.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Friend w1 = (Friend) a;
        Friend w2 = (Friend) b;
        return w1.compareTo(w2);
    }
}
