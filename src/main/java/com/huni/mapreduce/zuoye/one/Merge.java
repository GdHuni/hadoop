package com.huni.mapreduce.zuoye.one;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Merge {


    public static class MergeMap extends Mapper<Object, Text, Text, Text> {

        private Text line = new Text();//每一行作为一个数据
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            context.write(value, new Text(""));//把每一行都放到text中 唯一的key实现了去重
        }

    }



    public static class MergeReduce extends Reducer<Text, Text, Text, Text> {

        @Override

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            context.write(key, new Text(""));

        }

    }


    public static void main(String[] args) throws Exception {
        //1.获取配置信息，得到Job对象
        Configuration configuration =new Configuration();
        Job job = Job.getInstance(configuration);


        //2.设置加载jar的位置
        job.setJarByClass(Merge.class);

        //3.设置mapper和reduce的class类
        job.setMapperClass(MergeMap.class);
        job.setReducerClass(MergeReduce.class);



        //设置最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //判断输出路径，是否存在，如果存在，则删除
        Path path = new Path("e://out");
        FileSystem fileSystem = path.getFileSystem(configuration);
        //getFileSystem()函数功能  Return the FileSystem that owns this Path.
        if (fileSystem.exists(new Path("e://out"))) {
            fileSystem.delete(new Path("e://out"),true);
        }

        //设置输入数据和输出数据的的路径
        FileInputFormat.setInputPaths(job, new Path("e://input1"));
        FileOutputFormat.setOutputPath(job, new Path("e://out"));

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }



}