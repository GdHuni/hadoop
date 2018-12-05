package com.huni.mapreduce.Distribute;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @功能描述:
 * @项目版本:
 * @项目名称:
 * @相对路径: com.huni.mapreduce.Distribute
 * @创建作者: 周虎
 * @创建日期: 2018/12/4 17:00
 */
public class DistributedcacheJob {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        //1.获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置ja的位置
        job.setJarByClass(DistributedcacheJob.class);

        //3.设置map的位置
        job.setMapperClass(DistributedcacheMapper.class);

        //4.最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //5.设置输入路径
        FileInputFormat.setInputPaths(job,new Path("g:/order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("g:/x"));

        //6.加载缓存数据
        job.addCacheFile(new URI("file:/g:/h.txt"));

        // 7 map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        // 8 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
