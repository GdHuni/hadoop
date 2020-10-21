package com.huni.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @功能描述:
 * @相对路径: com.huni.hbase.HbaseUtil
 * @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
 * @创建日期: 2020/10/21 0021 14:56
 */
public class HbaseUtil {
    private Logger log = LoggerFactory.getLogger(HbaseUtil.class);

    /**
     * 声明静态配置
     */
    private static Connection connection = null;
    private static Configuration conf = null;
    private static HBaseAdmin admin = null;
    public HbaseUtil(){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata182");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            log.error("获取HBase连接失败");
        }
    }

    /**
    * @功能描述: 插入数据
    * @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
    * @创建日期:  2020/10/21 0021 15:34
     *@param:  表名，rowkey，列族，列名，值
    *
    */
    public void putData(String tableName,String rowKey,String familyName,String column,String value) throws IOException {
        Table t = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(value));
        //执行插入
        t.put(put);
        // t.put();//可以传入list批量插入数据
        //关闭table对象
        t.close();
        log.info("插入成功");
    }


    public void createTable(String tableName,String familyName) throws IOException {
        admin = (HBaseAdmin) connection.getAdmin();
        //创建表描述器
        HTableDescriptor teacher = new HTableDescriptor(TableName.valueOf(tableName));
        //设置列族描述器
        teacher.addFamily(new HColumnDescriptor(familyName));
        //执行创建操作
        admin.createTable(teacher);
        System.out.println("teacher表创建成功！！");
    }

    public void deleteData(String tableName,String rowKey) throws IOException {
        //需要获取一个table对象
        final Table worker = connection.getTable(TableName.valueOf(tableName));
        //准备delete对象
        final Delete delete = new Delete(Bytes.toBytes(rowKey));
        //执行删除
        worker.delete(delete);
        //关闭table对象
        worker.close();
        System.out.println("删除数据成功！！");
    }

    public void getDataByCF(String tableName,String rowKey,String familyName) throws IOException {
        //获取表对象
        HTable teacher = (HTable) connection.getTable(TableName.valueOf(tableName));
        //创建查询的get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //指定列族信息
        // get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));
        get.addFamily(Bytes.toBytes(familyName));
        //执行查询
        Result res = teacher.get(get);
        //获取改行的所有cell对象
        Cell[] cells = res.rawCells();
        for (Cell cell : cells) {
            //通过cell获取rowkey,cf,column,value
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            System.out.println(rowkey + "----" + cf + "---" + column + "---" + value);
        }
        //关闭表对象资源
        teacher.close();
    }

    public void scanAllData(String tableName) throws IOException {
        HTable teacher = (HTable) connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = teacher.getScanner(scan);
        for (Result result : resultScanner) {
            //获取改行的所有cell对象
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //通过cell获取rowkey,cf,column,value
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.println(rowkey + "----" + cf + "--" + column + "---" + value);
            }
        }
        teacher.close();
    }

    /**
    * @功能描述: 通过startRowKey和endRowKey进行扫描查询
    * @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
    * @创建日期:  2020/10/21 0021 16:04
     *@param:
    *
    */
    public void scanRowKey(String tableName,String startRowKey,String endRowKey) throws IOException {
        HTable teacher = (HTable) connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(startRowKey.getBytes());
        scan.setStopRow(endRowKey.getBytes());
        ResultScanner resultScanner = teacher.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();//获取改行的所有cell对象
            for (Cell cell : cells) {
                //通过cell获取rowkey,cf,column,value
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.println(rowkey + "----" + cf + "--" + column + "---" + value);
            }
        }
        teacher.close();
    }

    public void destroy() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
