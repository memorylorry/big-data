package com.github.memorylorry.cocurrent;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class Print implements Runnable{
	Thread thread;
	int i;
	public Print(Thread thread, int i){
		this.thread = thread;
		this.i = i;
	}

	@Override
	public void run() {
		try {
			if(thread!=null)
				thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println(i);
	}
}

public class Join {
	public static void main(String[] args) throws InterruptedException {
		Thread thread = Thread.currentThread();

		for(int i=0; i<10; i++){
			thread = new Thread(new Print(thread, i));
			thread.start();
		}

		System.out.println("main end!");

	}

}
