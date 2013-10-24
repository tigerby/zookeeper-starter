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

    public static void main (String[] args ) throws Exception {
        int sessionTimeout = 10 * 1000;

        // 1. ZooKeeper 서벼(클러스터)에 연결
        ZooKeeper zk = new ZooKeeper ("tiger01:2181,tiger02:2181,tiger03:2181", sessionTimeout, null);

        // 2. /testO1 znode가 존재하지 않으연 /testO1, /test02 생성
        if (zk.exists ("/test01", false) == null) {
            zk.create("/test01", "test01_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/test02", "test02_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 3. /testO1 노드의 자식 노드로 subO1, sub02 생성 (EPHEMERAL 노드)
        zk.create("/test01/sub01", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.create("/test01/sub02", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // 4. /test01 노드의 데이터 가져오기
        byte[] test01Data = zk.getData("/test01", false , null);
        System.out.println("getData[/test01]: " + new String(test01Data));

        // 5. /test01/sub01 노드의 데이터를 새로운 값으로 설정
        zk.setData("/test01/sub01", "this new data".getBytes() , -1);
        byte[] subData = zk.getData("/test01/sub01", false, null);
        System.out.println("getData after setdata [/test01/sub01]:" + new String(subData));

        // 6. 노드가 존재하는지 확인
        System.out.println("exists[/test01/sub01]:" + (zk.exists("/test01/sub01", false) != null));
        System.out.println("exists[/test01/sub03]:" + (zk.exists("/test01/sub03", false) != null));

        // 7. Itest01의 자식 노드 목록 가져오기
        List<String> children = zk.getChildren("/test01", false);
        for (String eachChildren: children) {
            System.out .println ("getChildren [!test 01 ] : " + eachChildren);
        }

        // 8. ZK 접속종료
        zk.close();
    }
}
