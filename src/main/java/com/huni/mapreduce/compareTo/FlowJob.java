package com.huni.mapreduce.compareTo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowJob {
    public static void main(String[] args) throws Exception {
        //1.创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.获取jar运行路径
        job.setJarByClass(FlowJob.class);
        //3.获取mapper类
        job.setMapperClass(FlowMapper.class);
        //4.获取reduce类
        job.setReducerClass(FlowReducer.class);
        //5.获取mapper输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //6.获取最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //7.获取输入输出数据路径
        //如果需要运行本地的文件就要去掉config中的core-site.xml文件
        FileInputFormat.setInputPaths(job,new Path("e://part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("e://output"));
        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
