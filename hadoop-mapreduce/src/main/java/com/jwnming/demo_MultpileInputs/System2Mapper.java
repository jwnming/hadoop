package com.jwnming.demo_MultpileInputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class System2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
        String[] tokens = value.toString().split(" ");
        String userId = tokens[2];
        String name = tokens[4];
        double cost = Double.parseDouble(tokens[0]) * Integer.parseInt(tokens[1]);
        context.write(new Text(userId), new Text(name + ":" + cost));


    }
}
