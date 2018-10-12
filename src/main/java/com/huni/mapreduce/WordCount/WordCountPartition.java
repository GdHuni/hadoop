package com.huni.mapreduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 类中的参数要与map的输出类型一致
 */
public class WordCountPartition extends Partitioner<Text,IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        //根据自己的业务来写，这里是将读取来的数据转换小写
        String word = text.toString().toLowerCase();
        //截取每个单词的首字母转换成char类型
        char[] words = word.substring(0, 1).toCharArray();
        byte num =0;
        for (char w:words){
            num = (byte)w;//将字母转换成数字askii码
        }
        //几个return 就有几个分区
        if(num<=100){
            return 0;
        }else {
            return 1;
        }
    }
}
