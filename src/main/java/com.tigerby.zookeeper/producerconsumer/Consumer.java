package com.tigerby.zookeeper.producerconsumer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class Consumer implements Watcher {
    private Integer mutex = 1;
    private String zkServers;
    private String consumerId;
    private ZooKeeper zk;
    private ExecutorService executor = Executors.newFixedThreadPool(10);
    private boolean stop = false;

    public Consumer(String zkServers, String consumerId) {
        this.zkServers = zkServers;
        this.consumerId = consumerId;
    }

    public void startConsumer()  {
        try {
            this.zk = new ZooKeeper(zkServers, 10 * 1000, this);

            synchronized (mutex) {
                mutex.wait();
            }
            System.out.println("Start consumer: " + consumerId);

            Producer.createQueueRoot(zk);

            Stat stat = new Stat();
            String queuePath = "/" + Producer.SERVICE_NAME + "/" + Producer.QUEUE_NAME;

            while(!stop) {
                synchronized (mutex) {
                    try {
                        List<String> children = zk.getChildren(queuePath, true);

                        for(String child: children) {
                            String childPath = queuePath + "/" + child;
                            byte[] queueData = null;
                            try {
                                queueData = zk.getData(childPath, false, stat);
                            } catch(KeeperException.NoNodeException e) {
                                continue;
                            }
                            try {
                                zk.delete(childPath, -1);
                            } catch(KeeperException.NoNodeException e) {
                                continue;
                            }

                            executor.submit(new ConsumerTask(new String(queueData)));
                        }

                        mutex.wait();
                    } catch (KeeperException.ConnectionLossException e) {
                        mutex.wait();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stopConsumer() {
        stop = true;
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    class ConsumerTask implements Runnable {
        String data;

        public ConsumerTask(String data) {
            this.data = data;
        }

        @Override
        public void run() {
            System.out.println("Consumer [" + consumerId + "] [" + data + "]");
        }
    }
}
