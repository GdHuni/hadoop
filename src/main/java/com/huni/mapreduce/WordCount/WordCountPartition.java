package com.huni.mapreduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartition extends Partitioner<Text,IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String word = text.toString().toLowerCase();
        char[] words = word.substring(0, 1).toCharArray();
        byte num =0;
        for (char w:words){
            num = (byte)w;
        }
        if(num<=100){
            return 0;
        }else {
            return 1;
        }
    }
}
