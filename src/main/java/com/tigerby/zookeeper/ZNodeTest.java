package com.tigerby.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class ZNodeTest {

  public static void main(String[] args) throws Exception {
    int sessionTimeout = 10 * 1000;
    ZooKeeper zk = new ZooKeeper("tiger01:2181,tiger02:2181,tiger03:2181", sessionTimeout, null);

    if (zk.exists("/test01", false) == null) {
      zk.create("/test01", "This is data 1.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }
    if (zk.exists("/test02", false) == null) {
      zk.create("/test02", "This is data 2.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    zk.create("/test01/child01", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    zk.create("/test01/child02", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    byte[] data1 = zk.getData("/test01", false, null);
    System.out.println("getData(\"/test01\"): " + new String(data1));

    zk.setData("/test01/child01", "This new data.".getBytes(), -1);
    byte[] subData = zk.getData("/test01/child01", false, null);
    System.out.println("getData() after setdata(\"/test01/child01\"): " + new String(subData));

    System.out.println("exists(\"/test01/child01\"): " + (zk.exists("/test01/child01", false) != null));
    System.out.println("exists(\"/test01/sub03\"): " + (zk.exists("/test01/sub03", false) != null));

    List<String> children = zk.getChildren("/test01", false);
    for (String eachChildren : children) {
      System.out.println("getChildren(\"/test01\"): " + eachChildren);
    }

    zk.close();
  }
}
