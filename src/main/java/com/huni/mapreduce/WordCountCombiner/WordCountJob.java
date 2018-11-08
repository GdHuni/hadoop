package com.huni.mapreduce.WordCountCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @作用：相当于一个yarn集群的客户端，需要在此封装我们的mr程序相关运行参数，
 * 		指定jar包,最后提交给yarn
 * @ClassName: WordCountJob
 * @author: Huni
 * @date: 2018年10月7日 下午4:14:17
 */
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //自定义设置分区
        job.setPartitionerClass(WordCountPartition.class);//自定义为自己重写的partition类
        job.setNumReduceTasks(2);//设置开启reduceTask的数量，要与分区数量一致
        //设置文件切割机制,当达到512k时才会去执行切割，最小512k,最大10m
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,10240);
        CombineTextInputFormat.setMinInputSplitSize(job,512);
        //设置combiner合并路径
        //主要是在map阶段就进行了一次汇总Combine input records=25 Combine output records=18，这个会有值了
        job.setCombinerClass(WordCountCombiner.class);
        //6.设置输入数据和输出数据的的路径
        FileInputFormat.setInputPaths(job, new Path("e://input"));
        FileOutputFormat.setOutputPath(job, new Path("e://out1"));
        //FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/input/install.log"));
       // FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/outt"));

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

}
