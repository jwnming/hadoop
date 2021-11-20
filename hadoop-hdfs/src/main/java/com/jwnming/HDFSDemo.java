package com.jwnming;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class HDFSDemo {
    private FileSystem fileSystem;

    @Before
    public void before() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://CentOS:9000");    // core-site.xml
        conf.set("dfs.replication", "1");
        fileSystem = FileSystem.get(conf);
    }

    //上传
    @Test
    public void testUpload() throws IOException {
        InputStream is = new FileInputStream("D:\\hadoop\\test.txt");
        Path path = new Path("/hdfs.txt");
        OutputStream os = fileSystem.create(path);
        IOUtils.copy(is, os);
        is.close();
        os.close();
    }

    //下载
    @Test
    public void testDownload() throws IOException {
        OutputStream os = new FileOutputStream("D:\\hadoop\\test1.txt");
        Path path = new Path("/hdfs.txt");
        InputStream is = fileSystem.open(path);
        org.apache.hadoop.io.IOUtils.copyBytes(is, os, 1024, true);
        is.close();
        os.close();
    }

    //下载
    @Test
    public void testDownload2() throws IOException {

        Path dst = new Path("D:\\hadoop\\test2.txt");
        Path src = new Path("/hdfs.txt");
        fileSystem.copyToLocalFile(src, dst);
    }

    //查询所有文件
    @Test
    public void testListFile() throws IOException {
        Path src = new Path("/");
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(src, true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            System.out.println(fileStatus.getPath());
        }
    }

    //文件状态(文件|目录)
    @Test
    public void testFileStatus() throws IOException {
        Path src = new Path("/");
        FileStatus[] fileStatuses = fileSystem.listStatus(src);
        for (FileStatus f : fileStatuses) {
            System.out.println(f.getPath());
        }
    }
}
