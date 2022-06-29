package com.huni.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class HbaseClientDemo {

    HbaseUtil hbaseUtil;

    @Before
    public void getConnection(){
        hbaseUtil = new HbaseUtil();
    }

    @Test
    public void createTable() throws IOException {
        hbaseUtil.createTable("my_test","info");
    }

    @Test
    public void putData() throws IOException {
        hbaseUtil.putData("user_house_view_log","18100283804","info",
                "rent_tag_comb","123");
    }


    @Test
    public void deleteData() throws IOException {
        hbaseUtil.deleteData("user_house_view_log","18100283804");
    }

    @Test
    public void getDataByCF() throws IOException {
        hbaseUtil.getDataByCF("user_house_view_log","18100283804","info");
    }

    @Test
    public void scanAllData() throws IOException {
      hbaseUtil.scanAllData("user_house_view_log");
    }

    @Test
    public void scanRowKey() throws IOException {
        hbaseUtil.scanRowKey("user_house_view_log","18100283804","000002_zf_4E4C7DB4-5B1F-4A2B-AA12-8097B4EEE7E4");
    }
}