package com.tigerby.zookeeper;

import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class HelloConfigServer implements Watcher {
    public static final String ZK_SERVERS = "127.0.0.1:2181";
    public static final int SESSION_TIMEOUT = 10 * 1000;
    public static final String HELLO_SERVICE_PATH = "/hello_service";
    public static final String CONF_PATH = HELLO_SERVICE_PATH + "/conf";
    public static final String CONF_GREETING_KEY = "hello.greeting";

    public static Map<String, String> configurations = new HashMap<String,String>();

    private ZooKeeper zk;
    private CountDownLatch connMonitor = new CountDownLatch(1);

    public HelloConfigServer() throws Exception {
        zk = new ZooKeeper(ZK_SERVERS, SESSION_TIMEOUT, this);
        connMonitor.await();
        if (zk.exists(CONF_PATH, false) == null) {
            try {
                zk.create(HELLO_SERVICE_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
            }

            zk.create(CONF_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<String> confNodes = zk.getChildren(CONF_PATH, this);
            for (String eachNode : confNodes ) {
                byte[] confData = zk.getData(CONF_PATH + "/" + eachNode, this, new Stat());
                if (confData != null) {
                    configurations.put (eachNode, new String(confData));
                }
            }
        }
    }


    public void startServer (int port) throws Exception {
        final TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
//        final HelloService.Processor processor = new HelloService.Processor (new HelloHandler());
//        final TServer server = new THsHaServer(processor, socket, new TFramedTransport.Factory(), new TBinaryProtocol.Factory()) ;
        System.out.println("HelloServer started(port: " + port + ")");
//        server.serve();
    }

    @Override
    public void process (WatchedEvent event ) {
        if (event.getType() == Event .EventType .None) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                connMonitor.countDown();
            }
        } else if(event.getType() == Event.EventType.NodeChildrenChanged) {
            if(event.getPath().equals(CONF_PATH)) {
                reloadConfigurations();
            }
        } else if (event.getType() == Event.EventType.NodeDataChanged) {
            if (event.getPath().startsWith(CONF_PATH)) {
                String path = event.getPath ();
                if(path.lastIndexOf("/") >= 0) {
                    path = path.substring(path.lastIndexOf("/") + 1);
                }

                try {
                    byte[] confData = zk.getData(event.getPath(), this, new Stat());
                    synchronized(configurations) {
                        if(confData != null) {
                            configurations.put(path, new String(confData));
                        }
                    }
                } catch (Exception e) {
                    e .printStackTrace () ;
                }
            }
        }
    }

    private void reloadConfigurations () {
        try {
            List<String> children = zk.getChildren(CONF_PATH, this);

            List<String> addedNodes = new ArrayList<String>();
            List<String> removedNodes = new ArrayList<String>();

            findChangedChildren(configurations.keySet(), children, addedNodes, removedNodes);

            synchronized (configurations) {
                for(String eachNode : removedNodes) {
                    configurations.remove (eachNode) ;
                }

                for (String eachNode : addedNodes ) {
                    byte[] confData = zk.getData(CONF_PATH + "/" + eachNode, false, new Stat());
                    if(confData != null) {
                        configurations.put(eachNode, new String(confData ) ) ;
                    }
                }
            }
        } catch(Exception e) {
            e .printStackTrace() ;
        }
    }

    public static void findChangedChildren (
            Collection<String> cachedDatas,
            Collection<String> currentChildren,
            Collection<String> addedNodes,
            Collection<String> removedNodes) throws Exception {

        Set<String> cachedSet = new HashSet<String>() ;
        cachedSet .addAll (cachedDatas ) ;

        for(String eachNode : currentChildren ) {
            if(!cachedSet.remove(eachNode)) {
                removedNodes .add (eachNode) ;
            }
        }

        addedNodes.addAll(cachedSet) ;
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage java HelloServer <port>");
            System.exit(0) ;
        }

        new HelloConfigServer().startServer(Integer.parseInt(args[0]));
    }

//    class HelloHandler implements HelloService.Iface {
//        @Override
//        public String greeting(String name, int age) throws TException {
//            String greetingPrefix = "Hello";
//
//            synchronized(HelloConfigServer.configurations) {
//                if (HelloConfigServer. configurations .containsKey(HelloConfigServer.CONF_GREETING_KEY)) {
//                    greetingPrefix = HelloConfigServer.configurations .get(HelloConfigServer.CONF_GREETING_KEY);
//                }
//            }
//            return greetingPrefix + " " + name +" You are " + age + " years old";
//        }
//    }
}
