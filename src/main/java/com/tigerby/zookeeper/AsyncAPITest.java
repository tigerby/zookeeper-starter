package com.tigerby.zookeeper;

import org.apache.zookeeper.*;

/**
 * zookeeper asynchronous call todo need to make it success. currently it is always back with
 * failure.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class AsyncAPITest implements Watcher {

  private ZooKeeper zk;
  private Object monitor = new Object();

  class CreateCallBack implements AsyncCallback.StringCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc == 0) {
        System.out.println(
            "success! " + " path: " + path + ", ctx: " + ctx + ", name: " + name);
      } else {
        System.out.println(
            "fail! " + "rc: " + rc + ", path: " + path + ", ctx: " + ctx + ", name: " + name);
      }

      synchronized (monitor) {
        monitor.notifyAll();
      }
    }
  }

  public void start() {
    try {
      zk = new ZooKeeper("tiger01:2181,tiger02:2181,tiger03:2181", 5000, this);
      synchronized (monitor) {
        monitor.wait();
      }

      // add callback class
      zk.create("/async/test", "async test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

      System.out.println("Do something asynchronously");

      // wait until callback function is done.
      synchronized (monitor) {
        monitor.wait();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    synchronized (monitor) {
      monitor.notify();
      System.out.println("connected to zookeeper: " + event.getState());
    }
  }

  public static void main(String[] args) {
    new AsyncAPITest().start();
  }
}
