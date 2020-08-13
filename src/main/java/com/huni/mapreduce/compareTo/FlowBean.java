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

    /**
     * compareTo原理·
     第一步：首先要知道 Collections.sort()方法进行排序的时候，sort里面默认是升序排序。这里一定要记住了。
     第二步:  compare函数的返回值-1、1、0是什么个意思?
     返回值为-1, 表示左边的数比右边的数小，左右的数不进行交换。
     返回值为0, 表示左边的数等于右边的数，左右的数不进行交换。
     返回值为1, 表示左边的数比右边的数大，左右的数进行交换。(不进行交换的话，就没办法维持升序)
     上面的例子中this.age可以理解成左边的数，o.age可以理解成右边的数。
     this.age-o.age>0 说明左边的数比右边的数大，return this.age-o.age 返回的是一个正数，就进行左右交换，所以最终输出是升序。
     this.age-o.age<0 说明左边的数比右边的数小，return this.age-o.age 返回的是一个负数，不用进行交换，所以最终输出是升序。
     * @param o
     * @return
     */
    // 所以重写比较方法,-1为倒序
    @Override
    public int compareTo(FlowBean o) {
        return this.sumFlow>o.getSumFlow() ? -1:1;
    }
}
