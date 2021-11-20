package com.jwnming.demo_KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomJobSubmitter extends Configured implements Tool {

    //封装任务
    public int run(String[] strings) throws Exception {
        //1 装任务对象
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CustomJobSubmitter.class);

        //2 设置数据处理格式（读入|写出）
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //3 设置数据的路径信息（读入|写出）
        /*  Path src = new Path("demo/click");
        TextInputFormat.addInputPath(job, src);

        Path dst = new Path("/demo/result");
        TextOutputFormat.setOutputPath(job, dst);*/

        //本地仿真
        Path src = new Path("file:///D:/hadoop/demo/click");
        KeyValueTextInputFormat.addInputPath(job, src);
        Path dst = new Path("file:///D:/hadoop/demo/result2");
        TextOutputFormat.setOutputPath(job, dst);

        //4 设置数据的处理逻辑（map|reduce）
        job.setMapperClass(com.jwnming.demo_KeyValueTextInputFormat.ClickMapper.class);
        job.setReducerClass(com.jwnming.demo_KeyValueTextInputFormat.CliceReducer.class);

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
