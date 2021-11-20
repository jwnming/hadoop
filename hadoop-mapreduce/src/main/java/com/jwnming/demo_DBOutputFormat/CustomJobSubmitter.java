package com.jwnming.demo_DBOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomJobSubmitter extends Configured implements Tool {

    //封装任务
    public int run(String[] strings) throws Exception {
        //1 装任务对象
        Configuration conf = getConf();
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/", "root", "mysql123");

        Job job = Job.getInstance(conf);

        //2 设置数据处理格式（读入|写出）
        job.setOutputFormatClass(DBOutputFormat.class);

        //3 设置数据的路径信息（读入|写出）
        //本地仿真
        Path src = new Path("file:///D:/hadoop/demo/click");
        TextInputFormat.addInputPath(job, src);

        DBOutputFormat.setOutput(job, "t_order", "uid", "items", "total");

        //4 设置数据的处理逻辑（map|reduce）
        job.setMapperClass(ClickMapper.class);
        job.setReducerClass(CliceReducer.class);

        //5 说明map|reduce的输出key/values
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6 提交任务
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new CustomJobSubmitter(), args);
    }
}
