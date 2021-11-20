package com.jwnming.demo_MultpileInputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomJobSubmitter extends Configured implements Tool {

    //封装任务
    public int run(String[] strings) throws Exception {
        //1 装任务对象
        Configuration conf = getConf();
        //conf.set("mapreduc.input.lineinputformat.linespermap", "4");
        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomJobSubmitter.class);

        //2 设置数据处理格式（读入|写出）
        job.setOutputFormatClass(TextOutputFormat.class);

        //3 设置数据的路径信息（读入|写出）
        //本地仿真
        Path dst = new Path("file:///D:/hadoop/demo/result3");
        TextOutputFormat.setOutputPath(job, dst);

        MultipleInputs.addInputPath(job, new Path("file:///D:/hadoop/demo/order/system1"), TextInputFormat.class, System1Mapper.class);
        MultipleInputs.addInputPath(job, new Path("file:///D:/hadoop/demo/order/system2"), TextInputFormat.class, System2Mapper.class);

        //4 设置数据的处理逻辑（map|reduce）
        job.setReducerClass(com.jwnming.demo_MultpileInputs.OrderSystemReducer.class);

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
