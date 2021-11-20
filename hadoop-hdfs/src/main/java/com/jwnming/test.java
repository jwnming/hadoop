package com.jwnming;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class test {

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = null;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://CentOS:9000");    // core-site.xml
        conf.set("dfs.replication", "1");
        fileSystem = FileSystem.get(conf);

        InputStream is = new FileInputStream("D:\\hadoop\\test.txt");
        Path path = new Path("/hdfs.txt");
        OutputStream os = fileSystem.create(path);
        IOUtils.copy(is, os);
        is.close();
        os.close();

    }
}
