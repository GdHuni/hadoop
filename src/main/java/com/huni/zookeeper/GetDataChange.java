package com.huni.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

/**
 * 对节点下的数据进行监控
 */
public class GetDataChange {
    public static void main(String[] args) throws InterruptedException {
        ZkClient client = new ZkClient("linux121:2181");

        //设置⾃定义的序列化类型,否则会报错！！
        client.setZkSerializer(new ZkStrSerializer());
        //判断节点是否存在，不存在创建节点并赋值
        final boolean exists = client.exists("/huni");
        if (!exists) {
            client.createEphemeral("/huni", "123");
        }

        //获取到zkClient
        client.subscribeDataChanges("/huni", new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                //定义接收通知之后的处理逻辑
                System.out.println(dataPath + " data is changed ,new data " +
                        data);
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println(dataPath + " is deleted!!");
            }
        });
        //更新节点的数据，删除节点，验证监听器是否正常运⾏
        Object o = client.readData("/huni");
        System.out.println(o);
        client.writeData("/huni", "new data");
        System.out.println( client.readData("/huni").toString());
        Thread.sleep(1000);
        //删除节点
        client.delete("/huni");
        Thread.sleep(Integer.MAX_VALUE);

    }


}
