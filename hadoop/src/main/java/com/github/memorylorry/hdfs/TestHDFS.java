package com.github.memorylorry.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHDFS {
    Configuration conf;
    FileSystem fs;

    @Before
    public void conn() throws IOException {
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/test");
        if(fs.exists(path)){
            fs.delete(path, true);
        }else{
            fs.mkdirs(path);
        }
    }

    @Test
    public void upload() throws IOException {
        Path path = new Path("/test/pom.xml");
        FSDataOutputStream os = fs.create(path);

        InputStream in = new FileInputStream(new File("/home/darling/Desktop/segnet_3.py"));
        IOUtils.copyBytes(in, os, conf, true);
    }

    @Test
    public void download() throws IOException {
        Path path = new Path("/test/pom.xml");
        FSDataInputStream is = fs.open(path);

        OutputStream os = new FileOutputStream(new File("/home/darling/Desktop/sss.py"));
        IOUtils.copyBytes(is, os, conf, true);
    }

    @Test
    public void blks() throws IOException {
        Path path = new Path("/big.txt");
        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());

        for(BlockLocation b:blocks){
            System.out.println(b);
        }
    }

}
