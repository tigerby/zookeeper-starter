package com.tigerby.zookeeper.groupmembers;

import com.tigerby.thrift.generated.HelloService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.zookeeper.*;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class HelloServer implements Watcher {
    static final int ZK_SESSION_TIMEOUT = 30 * 1000;
    static final String ZK_SERVERS = "tiger01:2181,tiger02:2181,tiger03:2181";
    static final String HELLO_GROUP = "/helloserver";

    private ZooKeeper zk ;
//    CountDownLatch connMonitor = new CountDownLatch(1);

    public void runServer(int port) throws Exception {
        final TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        final HelloService.Processor processor = new HelloService.Processor(new HelloHandler());
        final TServer server = new THsHaServer(new THsHaServer.Args(socket).processor(processor));

        System.out.println("HelloServer started (port: " + port + ")");
        server.serve();

        serverStarted(port);
    }

    private void serverStarted(int port) throws Exception {
        zk = new ZooKeeper(ZK_SERVERS, ZK_SESSION_TIMEOUT, this);

        if(zk.exists(HELLO_GROUP, false) == null) {
            zk.create(HELLO_GROUP, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
        }

        String serverInfo = InetAddress.getLocalHost().getHostName() + ":" + port;
        zk.create(HELLO_GROUP + "/" + serverInfo , null , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive ZK event: " + event);
    }

    public class HelloHandler implements HelloService.Iface {
        @Override
        public String greeting(String name, int age) throws TException {
            return "Hello " + name +"! You age is " + age + ".";
        }
    }

    public static void main(String[] args) throws Exception {
        new HelloServer().runServer(10001);

    }

}