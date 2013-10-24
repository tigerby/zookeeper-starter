package com.tigerby.zookeeper;

import org.apache.zookeeper.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class ConfigurationManager implements Watcher {
    private ZooKeeper zk;

    // ...


    public ConfigurationManager() throws Exception {
        zk = new ZooKeeper(HelloConfigServer.ZK_SERVERS, HelloConfigServer.SESSION_TIMEOUT, this);

        // ...

    }

    public void setConfValue(String key, String value) throws Exception {
        String confPath = HelloConfigServer.CONF_PATH + "/" + key;
        if(zk.exists(confPath, false) == null) {
            zk.create(confPath, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(confPath, value.getBytes(), -1);
            System.out.println("Data changed: " + confPath + ", " + value);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // ...
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.out.println("Usage: java ConfigurationgManager <key> <value>");
            System.exit(0);
        }

        new ConfigurationManager().setConfValue(args[0], args[1]);
    }
}
