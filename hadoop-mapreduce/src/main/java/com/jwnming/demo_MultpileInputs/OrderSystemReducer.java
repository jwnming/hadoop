package com.jwnming.demo_MultpileInputs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrderSystemReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        List<String> orders = new ArrayList<String>();
        double totalCost = 0.0;
        for (Text value : values) {
            String[] tokens = value.toString().split(":");
            orders.add(tokens[0]);
            totalCost += Double.parseDouble(tokens[1]);
        }
        context.write(key, new Text(orders + " " + totalCost));
    }
}
