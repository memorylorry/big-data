package com.github.memorylorry.mr.friendRecommend2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Friend implements WritableComparable<Friend> {

    private String name;
    private String friend;
    private int weight;

    @Override
    public String toString() {
        return "name='" + name + '\'' +
                ", friend='" + friend + '\'' +
                ", weight=" + weight;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFriend() {
        return friend;
    }

    public void setFriend(String friend) {
        this.friend = friend;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public int compareTo(Friend that) {
        int c1 = this.name.compareTo(that.name);
        if(c1 == 0){
            return -Integer.compare(this.weight, that.weight);
        }
        return c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(name);
        out.write('\n');
        out.writeChars(friend);
        out.write('\n');
        out.writeInt(weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readLine();
        System.out.println(this.name);
        this.friend = in.readLine();
        this.weight = in.readInt();
    }
}
