package com.github.memorylorry.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.retry.RetryNTimes;

import java.util.Arrays;

public class Ticket12306 implements Runnable{
	private static Integer tickets = 10;
	private InterProcessLock lock;

	public Ticket12306(){
		CuratorFramework client = CuratorFrameworkFactory.newClient("n1:2181,n2:2181,n3:2181", new RetryNTimes(10, 3000));
		client.start();

		// 初始化分布式锁
		this.lock = new InterProcessMultiLock(client, Arrays.asList("/diy/app1"));
	}

	@Override
	public void run() {
		try {
			lock.acquire();

			Thread.sleep(3000);

			System.out.println(String.format("%s get ticket %d", Thread.currentThread().getName(), tickets));
			tickets = tickets - 1;
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				lock.release();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
