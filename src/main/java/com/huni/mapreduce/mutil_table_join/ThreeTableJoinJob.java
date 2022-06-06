
package com.huni.mapreduce.mutil_table_join;
 
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 *
 * @作用：相当于一个yarn集群的客户端，需要在此封装我们的mr程序相关运行参数，
 * 		指定jar包,最后提交给yarn
 * @ClassName: WordCountJob
 * @author: Huni
 * @date: 2022年06月06日 下午4:14:17
 */
public class ThreeTableJoinJob {
	public static void main(String[] args) throws Exception {
		//1.获取配置信息，得到Job对象
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);

		//2.设置加载jar的位置
		job.setJarByClass(ThreeTableJoinJob.class);
		//3.设置mapper的class类（不需要reduce类 采用的mapjoin）
		job.setMapperClass(ThreeTableJoinMapper.class);

		//4.设置输出mapper的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);

		//输入输出文件 自己去定义
		String inputpath = "e://bigdata_input//select.txt";
		String outpath = "e://out";

		//因为学生表和课程表在实际中就很小所以将两个文件缓存到内存中
		URI uri1 = new URI("file:/e://bigdata_input//class.txt");
		URI uri2 = new URI("file:/e://bigdata_input//student.txt");
		job.addCacheFile(uri1);
		job.addCacheFile(uri2);
		Path inputPath = new Path(inputpath);
		Path outputPath = new Path(outpath);
		//判断输出目录是否存在，存在就删除
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		//6.设置输入数据和输出数据的的路径
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//7.提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	

 
}