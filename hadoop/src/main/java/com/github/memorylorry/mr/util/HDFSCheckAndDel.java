package com.github.memorylorry.mr.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSCheckAndDel {

	public static void check(String fp) throws IOException {
		Configuration conf = new Configuration(true);
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path(fp);
		if(fs.exists(path)){
			fs.delete(path, true);
			System.out.println(String.format("warn: delete %s!", fp));
		}
		fs.close();
	}


}
