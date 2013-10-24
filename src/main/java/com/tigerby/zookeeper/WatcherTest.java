package com.tigerby.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class WatcherTest implements Watcher {
    private static final String ZK_HOSTS = "tiger01:2181,tiger02:2181,tiger03:2181";
    private static final int ZK_SESSION_TIMEOUT = 60 * 1000;
    private ZooKeeper zk ;

    Object monitor = new Object();
//    CountDownLatch connMonitor = new CountDownLatch(1);

    private WatcherTest () throws Exception {
        zk = new ZooKeeper(ZK_HOSTS, ZK_SESSION_TIMEOUT, this);
//        connMonitor.await();
        SampleNodeWatcher nodeWatcher = new SampleNodeWatcher();
        zk.exists("/test01", nodeWatcher);
    }

    private void execute() {
//        while (true) {
//            try {
//                Thread.sleep(5 * 1000);
//                long sid = zk.getSessionId();
//                byte[] passwd = zk.getSessionPasswd();
//                zk.close();
//                zk = new ZooKeeper(ZK_HOSTS ,ZK_SESSION_TIMEOUT ,this ,sid ,passwd);
//            } catch (Exception e) {
//                e .printStackTrace();
//            }
//        }

        synchronized (monitor) {
            try {
                monitor.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    System.out.println ("ZK SyncConnected");
//                    connMonitor.countDown();
                    break;
                case Disconnected:
                    System.out .println ("ZK Disconnected");
                    break;
                case Expired:
                    System.out.println ("ZK Session Expired");
                    break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        WatcherTest test = new WatcherTest();
        test.execute();
    }

    class SampleNodeWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeCreated) {
                System.out.println("Receive NodeCreated event: " + event.getPath());
            } else if(event.getType() == Event.EventType.NodeDeleted) {
                System.out.println("Receive NodeDeleted event : " + event .getPath ());
            } else if(event.getType() == Event.EventType.NodeDataChanged) {
                System.out.println("Receive NodeDataChanged event: " +event.getPath());
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                System.out.println("Receive NodeChildrenChanged event: " + event .getPath () ) ;
            }
        }
    }
}
