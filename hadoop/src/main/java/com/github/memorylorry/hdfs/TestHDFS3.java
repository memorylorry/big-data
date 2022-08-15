package com.github.memorylorry.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestHDFS3 {

    Configuration conf;
    FileSystem fs;

    @Before
    public void start() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    @After
    public void stop() throws IOException {
        fs.close();
    }

    @Test
    public void test() throws IOException {
        Path  path = new Path("/big.txt");
        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
        BlockLocation block = blocks[1];

        FSDataInputStream fis =  fs.open(path);
        fis.seek(1);
        LineReader reader = new LineReader(fis);

        Text text = new Text();
        reader.readLine(text);
        System.out.println(text.toString());
        reader.readLine(text);
        System.out.println(text.toString());


    }

}
