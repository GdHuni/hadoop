package com.huni.mapreduce.flowConbine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    Text t = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        System.out.println(split);
        String pNum = split[0];
        long upFlow = Long.parseLong(split[1]);
        long downFlow = Long.parseLong(split[2]);
        flowBean.set(upFlow,downFlow);
        t.set(pNum);
        context.write(flowBean,t);
    }
}
