package com.github.memorylorry.zookeeper.lock;

public class Main {
	public static void main(String[] args) {
		for(int i=0; i<10; i++){
			new Thread(new Ticket12306(), "t"+i).start();
		}
	}
}
