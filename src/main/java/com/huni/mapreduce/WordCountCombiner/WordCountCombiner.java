package com.huni.mapreduce.WordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //Iterable<IntWritable> values是将相同的key的value存储在一个Iterable类型中，相当于(key-list)
        int count = 0;
        // 1 汇总各个key的个数values(key, value-list)
        for (IntWritable value : values) {
            count+=value.get();
        }
        // 2输出该key的总次数
        context.write(key, new IntWritable(count));
    }
}
