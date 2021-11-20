package com.jwnming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class HbaseTest {
    private Connection conn;
    private Admin admin;

    @Before
    public void before() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "CentOS");
        conn = ConnectionFactory.createConnection(config);
        admin = conn.getAdmin();

    }

    @Test
    public void test() throws IOException {
        //创建namespace
        NamespaceDescriptor nd = NamespaceDescriptor.create("jwnming").addConfiguration("author", "jwm").build();
        admin.createNamespace(nd);

        //创建表
        TableName tname = TableName.valueOf("jwnming:t_user");
        HTableDescriptor t_user = new HTableDescriptor(tname);
        //构建列簇
        HColumnDescriptor cf1 = new HColumnDescriptor("cf1");
        cf1.setMaxVersions(3);
        HColumnDescriptor cf2 = new HColumnDescriptor("cf2");
        cf2.setTimeToLive(10);
        //添加列簇
        t_user.addFamily(cf1);
        t_user.addFamily(cf2);
        admin.createTable(t_user);


        //插入数据
        TableName tableName1 = TableName.valueOf("jwnming_t_admin");
        Table t_admin = conn.getTable(tableName1);
        String[] company = {"www.baidu.com", "www.sina.com"};
        for (int i = 0; i < 1000; i++) {
            String com = company[new Random().nextInt(2)];
            String rowKey = com;
            if (i < 10) {
                rowKey += ":00" + i;
            } else if (i < 100) {
                rowKey += ":0" + i;
            } else if (i < 1000) {
                rowKey += ":" + i;
            }

            Put put = new Put(rowKey.getBytes());
            put.addColumn("cf1".getBytes(), "name".getBytes(), ("user" + i).getBytes());
            put.addColumn("cf1".getBytes(), "age".getBytes(), Bytes.toBytes(i));
            put.addColumn("cf1".getBytes(), "salary".getBytes(), Bytes.toBytes(5000 + 1000 * i));
            put.addColumn("cf1".getBytes(), "company".getBytes(), com.getBytes());
            t_admin.put(put);
        }
        t_admin.close();
    }

    @After
    public void after() throws IOException {
        admin.close();
        conn.close();
    }

}
