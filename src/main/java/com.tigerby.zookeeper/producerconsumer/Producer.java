package com.tigerby.zookeeper.producerconsumer;

import org.apache.zookeeper.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class Producer implements Watcher {
    public static final String SERVICE_NAME = "twitter_crawl";
    public static final String QUEUE_NAME = "test";
    public static final String QUEUE_PREFIX = "q_";

    private String zkServers;
    private String producerId;

    private ZooKeeper zk;
    private Object mutex = new Object();


    public Producer(String zkServers, String producerId) {
        this.zkServers = zkServers;
        this.producerId = producerId;
    }

    public void startProducer() {
        try {
            zk = new ZooKeeper(zkServers, 10 * 1000, this);

            synchronized (mutex) {
                mutex.wait();
            }

            System.out.println("Start producer: " + producerId);
            String queuePath = "/" + SERVICE_NAME + "/" + QUEUE_NAME + "/" + QUEUE_PREFIX;

            long seqNum = 1;
            while(true) {
                try {
                    zk.create(queuePath, (producerId + seqNum).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                    seqNum++;
                } catch (KeeperException e) {
                    synchronized (mutex) {
                        mutex.wait();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public static void createQueueRoot(ZooKeeper zk) throws InterruptedException {
        try {
            zk.create("/" + SERVICE_NAME, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {

        }

        try {
            zk.create("/" + SERVICE_NAME + "/" + QUEUE_NAME, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {

        }
    }
}
