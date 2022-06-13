package com.huni.mapreduce.zuoye.two;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Sort {


        public static class SortMap extends Mapper<Object, Text, IntWritable, IntWritable>{
            private static IntWritable data = new IntWritable();
            public void map(Object key, Text value, Context context) throws
                    IOException,InterruptedException{
                String text = value.toString();
                data.set(Integer.parseInt(text));
                context.write(data, new IntWritable(1));
            } }



    //reduce 函数将 map 输入的 key 复制到输出的 value 上，然后根据输入的 value-list
    //中元素的个数决定 key 的输出次数,定义一个全局变量 line_num 来代表 key 的位次
    public static class SortReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private static IntWritable line_num = new IntWritable(1);
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            for(IntWritable val : values){
                context.write(line_num, key);
                line_num = new IntWritable(line_num.get() + 1);
            } } }


    public static void main(String[] args) throws Exception {
        //1.获取配置信息，得到Job对象
        Configuration configuration =new Configuration();
        Job job = Job.getInstance(configuration);


        //2.设置加载jar的位置
        job.setJarByClass(Sort.class);

        //3.设置mapper和reduce的class类
        job.setMapperClass(SortMap.class);
        job.setReducerClass(SortReduce.class);



        //设置最终输出的数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        //判断输出路径，是否存在，如果存在，则删除
        Path path = new Path("e://out");
        FileSystem fileSystem = path.getFileSystem(configuration);
        //getFileSystem()函数功能  Return the FileSystem that owns this Path.
        if (fileSystem.exists(new Path("e://out"))) {
            fileSystem.delete(new Path("e://out"),true);
        }

        //设置输入数据和输出数据的的路径
        FileInputFormat.setInputPaths(job, new Path("e://input2"));
        FileOutputFormat.setOutputPath(job, new Path("e://out"));

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }



}