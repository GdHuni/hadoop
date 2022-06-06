package com.huni.mapreduce.mutil_table_join;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ThreeTableJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		private static Map<String, String> classMap = new HashMap<>();
		private static Map<String, String> studentsMap = new HashMap<>();
		private static Text kout = new Text();
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			//加载缓存文件
			URI[] paths = context.getCacheFiles();
			//Path[] localCacheFiles = context.getLocalCacheFiles();//方法已过时，采用上面的方法
			//课程表数据结构 1,数学,8 课程表
			String classStr = paths[0].getPath();
			BufferedReader bf1 = new BufferedReader(new FileReader(new File(classStr)));
			String stringLine = null;
			while((stringLine = bf1.readLine()) != null){
				String[] reads = stringLine.split(",");
				String classId = reads[0];
				String classInfo = reads[1] + "," + reads[2];
				classMap.put(classId, classInfo);
			}
			//学生表数据结构 1,张三,17781777634,202
			String strstudent = paths[1].getPath();
			BufferedReader bf2 = new BufferedReader(new FileReader(new File(strstudent)));
			String stringLine2 = null;
			while((stringLine2 = bf2.readLine()) != null){
				String[] reads = stringLine2.split(",");
				String studentId = reads[0];
				String studentInfo =  reads[0]+ ","+reads[1] + "," + reads[2] + "," + reads[3];
				studentsMap.put(studentId, studentInfo);
			}
			//关闭资源
			IOUtils.closeStream(bf1);
			IOUtils.closeStream(bf2);
 
		}

		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String [] selectStrs = value.toString().trim().split(",");
			//数据格式1,1,2022-01-01 14:00,教室1
			//通过学号和classId在对应的map中获取信息，正常来说通过表的外键设置，ratings不存在空信息，如果存在空信息，需要进行map.contain判断,所以造数据的时候注意下
			String studentStr = studentsMap.get(selectStrs[0]);
			String classStr = classMap.get(selectStrs[1]);
			//进行三表拼接，数据格式：学号, 姓名, 课程名称, 学时, 上课时间, 上课地点
			String [] studentInfo = studentStr.split(",");
			String [] classInfo = classStr.split(",");
			String kk = studentInfo[0] +"," + studentInfo[1] + "," + classInfo[0] + "," + classInfo[1] + "," + selectStrs[2] + "," + selectStrs[3];
			kout.set(kk);
			context.write(kout, NullWritable.get());
		}
	}
 