package com.huni.mapreduce.HDFSAPI;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 *
 * @作用
 * @ClassName: HDFSAPI
 * @author: Huni
 * @date: 2018年7月26日 下午8:16:53
 */
public class HDFSAPI {
    //配置对象，提取配置信息
    private  static Configuration conf = new Configuration();
    private static FileSystem fs;
    public static void main(String[] args) throws IOException {
        writFile();
        //writFileReplication();
        //readFile0();
        //readFile1();

    }
    /**
     *
     * @作用:
     * @Title: writFile
     * @author Huni
     * @date 2018年7月26日下午8:18:42
     * @throws IOException void
     */
    public static  void writFile() throws IOException{
        fs = FileSystem.get(conf);
        Path path = new Path("hdfs://master:9000/data/hdfs.txt");
        //文件系统的数据输出流
        FSDataOutputStream dos = fs.create(path);
        dos.write("hellow hdfs".getBytes());
        dos.close();
        System.out.println("over");
    }
    /**
     *
     * @作用: 写文件指定副本数
     * @Title: writFileReplication
     * @author Huni
     * @date 2018年7月26日下午8:19:02
     * @throws IOException void
     */
    public static  void writFileReplication() throws IOException{
        fs = FileSystem.get(conf);
        Path path = new Path("hdfs://master:9000/data/hdfsReplication.txt");
        //文件系统的数据输出流
        FSDataOutputStream dos = fs.create(path,(short)3);
        dos.write("hellow hdfs".getBytes());
        dos.close();
        System.out.println("over");
    }
    /**
     *
     * @作用:  从HDFS上读取文件到本地,指定读取多少
     * @Title: readFile0
     * @author Huni
     * @date 2018年7月26日下午8:21:29 void
     */
    public static void readFile0(){
        try {
            fs = FileSystem.get(conf);
            Path path = new Path("hdfs://master:9000/hadoop-2.8.4.tar.gz");
            FSDataInputStream fis = fs.open(path);
            FileOutputStream fos = new FileOutputStream("d://hadoop-2.8.4.tar.gz.part0");
            byte[] b = new byte[1024*1024];
            int len = 1;
            for(int i=0;i<128;i++){
                fis.read(b);
                fos.write(b);
            }
            fis.close();
            fos.close();
            System.out.println("success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     *
     * @作用: 指定读取第二块设备
     * @Title: readFile1
     * @author Huni
     * @date 2018年7月26日下午9:35:33 void
     */
    public static void readFile1(){
        try {
            fs = FileSystem.get(conf);
            Path path = new Path("/hadoop-2.8.4.tar.gz");
            FSDataInputStream fis = fs.open(path);
            FileOutputStream fos = new FileOutputStream("d://hadoop-2.8.4.tar.gz.part1");
            //指定偏移量
            fis.seek(1024*1024*128);
            //hadoop的封装类
            IOUtils.copyBytes(fis, fos, 1024);
            fis.close();
            fos.close();
            System.out.println("success");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
