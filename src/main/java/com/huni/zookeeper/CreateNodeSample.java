package com.huni.zookeeper;


import org.I0Itec.zkclient.ZkClient;

/**
 * zookeeper连接客户端demo以及对节点的操作
 */
public class CreateNodeSample {
    public static void main(String[] args) {
        //创建一个zk实例
        ZkClient zkClient = new ZkClient("linux121:2181");
        System.out.println("连接成功");
        //创建节点,true则可以递归创建
        zkClient.createPersistent("/huni/secondNode",true);
        System.out.println("节点创建成功");
        //递归删除节点
        zkClient.deleteRecursive("/huni");
        System.out.println("节点删除成功");
    }
}
