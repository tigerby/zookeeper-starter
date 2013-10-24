package com.tigerby.zookeeper.groupmembers;

import com.tigerby.thrift.HelloService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.zookeeper.*;

import java.net.InetAddress;

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

    public void runServer(int port) throws Exception {
        final TNonblockingServerSocket socket = new TNonblockingServerSocket(port);
        final HelloService.Processor processor = new HelloService.Processor(new HelloHandler());
        final TServer server = new THsHaServer(processor, socket, new TFramedTransport.Factory(), new TBinaryProtocol.Factory());

        System.out.println("HelloServer started (port: " + port + ")");
        server.serve();
    }

    private void serverStarted(int port) throws Exception {
        // ZooKeeper 서버에 연결하는 객체 생성
        zk = new ZooKeeper (ZK_SERVERS, ZK_SESSION_TIMEOUT, this) ;
//        connMonitor.await();

        // 멤버심 정보 저장을 위한 최상위 노드 (/helloserver) 생성
        if(zk.exists(HELLO_GROUP, false) == null) {
            zk.create(HELLO_GROUP, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;

            // 현재 수행되는 HelloServer의 서버명:포트명으로 노드 생성
            String serverInfo = InetAddress.getLocalHost().getHostName() + ":" + port;
            zk.create(HELLO_GROUP + "/" + serverInfo , null , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // ZooKeeper 이벤트를 받는 메소드
        System.out.println("Receive ZK event: " + event);
    }

    public class HelloHandler implements HelloService.Iface {
        @Override
        public String greeting(String name, int age) throws TException {
            return "Hello " + name +"! You age is " + age + ".";
        }
    }

}