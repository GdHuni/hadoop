package com.huni.mapreduce.Distribute;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @功能描述:
 * @项目版本:
 * @项目名称:
 * @相对路径: com.huni.mapreduce.Distribute
 * @创建作者: 周虎
 * @创建日期: 2018/12/4 17:13
 */
public class DistributedcacheMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Map<String,String> pdmap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1.获取缓存文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("h.txt")));
        String line;
        while (StringUtils.isNotEmpty(line=reader.readLine())){
            String[] split = line.split("\t");
            pdmap.put(split[0],split[1]);
        }
        reader.close();
    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        //3.获取订单id
        String orderId = split[1];
        // 4 获取商品名称
        String pdName = pdmap.get(orderId);

        // 5 拼接
        k.set(line + "\t"+ pdName);

        // 6 写出
        context.write(k, NullWritable.get());
    }
}
