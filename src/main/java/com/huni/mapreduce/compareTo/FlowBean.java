package com.huni.mapreduce.compareTo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    // 反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }
    public void set(long upFlow,long downFlow){
        this.upFlow = upFlow;
        this.downFlow=downFlow;
        this.sumFlow=upFlow+downFlow;
    }
    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }
    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //需要用\t来分割
    @Override
    public String toString() {
        return
                upFlow +
                "\t" + downFlow +
                "\t" + sumFlow ;
    }

    /**
     *  序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     *  反序列化方法，接收参数的顺序要与序列化的顺序一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow=in.readLong();
        downFlow=in.readLong();
        sumFlow=in.readLong();
    }

    //重写比较方法,-1为倒序
    @Override
    public int compareTo(FlowBean o) {
        return this.sumFlow>o.getSumFlow() ? -1:1;
    }
}
