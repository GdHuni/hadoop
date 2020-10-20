package com.huni.zookeeper.distributelock;

/**
* @功能描述: zk实现分布式锁
* @创建作者: <a href="mailto:zhouh@leyoujia.com">周虎</a>
* @创建日期:  2020/10/20 0020 17:17
*
*/
public class DisLockTest {
    public static void main(String[] args) {
        //使⽤10个线程模拟分布式环境
        for (int i = 0; i < 10; i++) {
            //启动线程
            new Thread(new DisLockRunnable()).start();
        }
    }

    static class DisLockRunnable implements Runnable {
        @Override
        public void run() {
            //每个线程具体的任务，每个线程就是抢锁，
            final DisClient client = new DisClient();
            client.getDisLock();
            //模拟获取锁之后的其它动作
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //释放锁
            client.deleteLock();
        }
    }
}