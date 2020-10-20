package com.huni.zookeeper;

import org.I0Itec.zkclient.ZkClient;


/**
* @功能描述: 对节点进行监控
    1 监听器可以对不存在的⽬录进⾏监听
    2 监听⽬录下⼦节点发⽣改变，可以接收到通知，携带数据有⼦节点列表
    3 监听⽬录创建和删除本身也会被监听到
* @使用对象: 自助分析
* @接口版本: 1.7.4
* @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
* @创建日期:  2020/10/20 0020 17:00
 *@param:
*
*/
public class GetNodeChange {
    public static void main(String[] args) throws InterruptedException {
        //获取到zkClient
        ZkClient zkClient = new ZkClient("linux121:2181");
        //对/lag-client注册了监听器，监听器是⼀直监听 对指定⽬录进⾏监听(不存在⽬录:/lg-client)，指定收到通知之后的逻辑
        zkClient.subscribeChildChanges("/lg-client",
                //该⽅法是接收到通知之后的执⾏逻辑定义
                (parentPath,currentChilds) -> System.out.println(parentPath + " childs changes ,current childs "
                        + currentChilds));
        //使⽤zkClient创建节点，删除节点，验证监听器是否运⾏
        zkClient.createPersistent("/lg-client");
        //只是为了⽅便观察结果数据
        Thread.sleep(1000);

        zkClient.createPersistent("/lg-client/c1");
        Thread.sleep(1000);

        zkClient.delete("/lg-client/c1");
        Thread.sleep(1000);

        zkClient.delete("/lg-client");
        Thread.sleep(Integer.MAX_VALUE);
    }
}
