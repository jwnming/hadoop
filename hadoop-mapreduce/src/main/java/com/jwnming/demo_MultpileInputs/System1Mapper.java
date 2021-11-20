package com.jwnming.demo_MultpileInputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class System1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        String userId = tokens[4];
        String name = tokens[1];
        double cost = Double.parseDouble(tokens[2]) * Integer.parseInt(tokens[3]);
        context.write(new Text(userId), new Text(name + ":" + cost));
    }
}
