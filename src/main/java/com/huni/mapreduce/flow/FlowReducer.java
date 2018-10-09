package com.huni.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
       long upFlow_sum = 0;
       long downFlow_sum = 0;
        for (FlowBean bean:values){
            upFlow_sum = bean.getUpFlow();
            downFlow_sum = bean.getDownFlow();
        }
        context.write(new Text(key),new FlowBean(upFlow_sum,downFlow_sum));
    }
}
