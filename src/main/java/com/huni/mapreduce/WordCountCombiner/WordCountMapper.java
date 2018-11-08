package com.huni.mapreduce.WordCountCombiner;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *
 * @ClassName: WordCountMapper
 * @author: Huni
 * 	LongWritable:是mr框架所读到的一行文本的起始偏移量,Long
 * 			在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而是用LongWritable
 * 	Text:是mr框架所读到的一行文本内容
 *
 * 	Text:通过mapper输出的key类型(每个单词)
 * 	IntWritable:通过mapper输出的value类型(每个单词出现的数量)
 * @date: 2018年10月7日 下午3:52:52
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //用来接收原始数据中每行的数据,并且转换成字符串类型
        String line = value.toString();
        //通过空格来切割每行数据，用来确定每个单词
        String[] words = line.split(" ");
        //将单词遍历输出到mapper的缓冲区中
        for(String word:words){
            //再次又将Java中的String类型转换成Hadoop中的Text
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
