package com.huni.mapreduce.zuoye.three;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountJob {
    public static void main(String[] args) throws Exception {
        //1.获取配置信息，得到Job对象
        Configuration configuration =new Configuration();
        Job job = Job.getInstance(configuration);

        //2.设置加载jar的位置
        job.setJarByClass(WordCountJob.class);

        //3.设置mapper和reduce的class类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4.设置输出mapper的数据类型
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //判断输出路径，是否存在，如果存在，则删除
        Path path = new Path("e://out");
        FileSystem fileSystem = path.getFileSystem(configuration);
        //getFileSystem()函数功能  Return the FileSystem that owns this Path.
        if (fileSystem.exists(new Path("e://out"))) {
            fileSystem.delete(new Path("e://out"),true);
        }

        //6.设置输入数据和输出数据的的路径
        FileInputFormat.setInputPaths(job, new Path("e://input"));
        FileOutputFormat.setOutputPath(job, new Path("e://out"));
        //FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/input/install.log"));
       // FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/outt"));

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

}
