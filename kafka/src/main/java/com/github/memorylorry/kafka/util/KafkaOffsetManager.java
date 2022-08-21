package com.github.memorylorry.kafka.util;

import java.io.*;

public class KafkaOffsetManager {
	File conf;

	public KafkaOffsetManager(String path){
		this.conf = new File(path);
	}

	public long readOffset(){
		if(conf.exists()){
			FileInputStream in = null;
			try {
				in = new FileInputStream(conf.getPath());
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				String a = reader.readLine();

				reader.close();
				in.close();
				return Long.parseLong(a);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return 0;
			} catch (IOException e) {
				e.printStackTrace();
				return 0;
			}
		}

		return -1;
	}

	public void write(long offset){
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(conf.getPath());
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
			writer.write(String.valueOf(offset));
			writer.flush();

			writer.close();
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		KafkaOffsetManager manager = new KafkaOffsetManager("kafka_offset.log");
//		long a = manager.readOffset();
		manager.write(112);
//		System.out.println(a);
	}
}
