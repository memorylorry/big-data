package com.github.memorylorry.zookeeper.crud;

import org.apache.curator.RetryPolicy;
import org.apache.curator.SessionFailedRetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class Simple {
	CuratorFramework client = null;

	@Test
	public void create() throws Exception {
		client.create().forPath("/hu", "123456".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void create_e() throws Exception{
		// 生成临时节点
		client.create().withMode(CreateMode.EPHEMERAL).forPath("/txy");
	}

	@Test
	public void creater() throws Exception{
//		/hu不存在，直接创建会出错，需要调用creatingParentsIfNeeded开启允许递归创建
//		client.create().forPath("/hu/app1", "123456".getBytes(StandardCharsets.UTF_8));
		client.create().creatingParentsIfNeeded().forPath("/hu/app1", "123456".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void ls() throws Exception {
		byte[] msg = client.getData().forPath("/");
		System.out.println(String.valueOf(msg));
	}

	@Test
	public void update() throws Exception {
		client.setData().forPath("/hu", "654321".getBytes(StandardCharsets.UTF_8));
	}

	@Test
	public void delete() throws Exception {
//		client.delete().forPath("/hu");
		client.delete().deletingChildrenIfNeeded().forPath("/hu");
	}

	@Before
	public void initial(){
		RetryPolicy policy = new RetryNTimes(10, 3000);
		client = CuratorFrameworkFactory.newClient("n1:2181,n2:2181,n3:2181", 5000, 2000, policy);
//		链式初始化方式
//		client = CuratorFrameworkFactory.builder().connectString("n1:2181;n2:2181;n3:2181").sessionTimeoutMs(5000).connectionTimeoutMs(2000).retryPolicy(policy).build();

		client.start();
	}

	@After
	public void close(){
		client.close();
	}
}
