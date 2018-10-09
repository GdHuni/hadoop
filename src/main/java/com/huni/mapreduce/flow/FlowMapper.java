package com.huni.mapreduce.flow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text t = new Text();
    FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.读取一行数据并且转换成String,并且按照规则切割
        String[] values = value.toString().split("\t");
        //2.取出手机号码，上行流量和下行流量
        String phoneName = values[1];
        long upFlow = Long.parseLong(values[values.length-3]);
        long downFlow = Long.parseLong(values[values.length-2]);
        //3.写出数据
        t.set(phoneName);
        flowBean.set(upFlow,downFlow);
        context.write(t,flowBean);
        //context.write(new Text(phoneName),new FlowBean(upFlow,downFlow));//这样写的话会多次new对象，浪费内存

    }
}
