package com.jwnming;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryUntilElapsed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ZooKeeperTest {
    //创建客户端
    private CuratorFramework client;

    @Before
    public void before() {
        String services = "CentOS:2181";
        RetryPolicy retryPolicy = new RetryNTimes(3, 10000);
        //在10秒内，每隔1秒尝试一次、
        new RetryUntilElapsed(10000, 1000);
        //尝试三次，每次递增1秒
        client = CuratorFrameworkFactory.newClient(services, retryPolicy);
        client.start();
    }

    //常规操作
    @Test
    public void test() throws Exception {

        client.setData().forPath("/jwnming", "我是蒋文明".getBytes());
        byte[] bytes = client.getData().forPath("/jwnming");
        System.out.println(new String(bytes));

    }


    @After
    public void after() {
        client.close();
    }

}
