package com.github.memorylorry.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHDFS2 {
    Configuration conf;
    FileSystem fs;

    @Before
    public void con() throws IOException {
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/0228");
        fs.mkdirs(path);
    }

    @Test
    public void upload() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/home/darling/big.txt")));
        Path outPath = new Path("/0228/big.txt");
        OutputStream out = fs.create(outPath, true);
        IOUtils.copyBytes(in, out, conf, true);
    }

    @Test
    public void blks() throws IOException {
        Path path = new Path("/0228/big.txt");
        FileStatus status = fs.getFileStatus(path);
        BlockLocation[] blocks =  fs.getFileBlockLocations(status, 0 , status.getLen());
        for(BlockLocation b:blocks){
            System.out.println(b);
        }
    }

    @Test
    public void lisDir() throws IOException {
        Path path = new Path("/0228");
        FileStatus status = fs.getFileStatus(path);

        if(status.isDirectory()){
            FileStatus[] statuses = fs.listStatus(path);
            for(FileStatus s:statuses){
                System.out.println(s);
            }
        }
    }

}
