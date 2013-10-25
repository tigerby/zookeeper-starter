package com.tigerby.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class ACLTest implements Watcher {

  public void start()
      throws NoSuchAlgorithmException, KeeperException, InterruptedException, IOException {
    ArrayList<ACL> acls = new ArrayList<ACL>();

    acls.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider
        .generateDigest("localhost:2181"))));
    acls.add(new ACL(ZooDefs.Perms.WRITE, new Id("ip", "10.1.1.2")));

    ZooKeeper zk = new ZooKeeper("tiger01:2181,tiger02:2181,tiger03:2181", 10 * 1000, this);

    zk.create("/acl_test", "This is data with acl.".getBytes(), acls, CreateMode.PERSISTENT);

    List<ACL> addedAcls = zk.getACL("/acl_test", new Stat());
    for (ACL eachAcl : addedAcls) {
      System.out.println(eachAcl.toString());
    }
  }

  public static void main(String[] args)
      throws NoSuchAlgorithmException, KeeperException, InterruptedException, IOException {
    new ACLTest().start();
  }

  @Override
  public void process(WatchedEvent event) {

  }
}
