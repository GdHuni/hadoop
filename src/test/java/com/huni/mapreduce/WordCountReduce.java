package com.huni.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @作用：
 * @ClassName: WordCountReduce
 * @author: Huni
 * Text:mapper阶段传过来的，输入中的key类型(每个的单词)
 * IntWritable：mapper阶段传过来的，输入中的value类型(每个单词出现的次数)
 * Text：reduce输出阶段的key类型(每个单词)
 * IntWritable：reduce输出阶段的value类型(统计之后每个单词出现的次数)
 * @date: 2018年10月7日 下午4:04:32
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
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