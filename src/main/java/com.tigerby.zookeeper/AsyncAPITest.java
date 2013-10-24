package com.tigerby.zookeeper;

import org.apache.zookeeper.*;

/**
 * Created with IntelliJ IDEA.
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
            if(rc == 0) {
                System.out.println("콜백 수신 성공");
            } else {
                System.out.println("콜백 수신 실패");
            }

            //callback 받으면 태스트 프로그램을 종료시키기 위해 main 흐름으로 알려준다
            synchronized (monitor) {
                monitor.notifyAll();
            }
        }
    }

    public void start (){
        try {
            zk = new ZooKeeper("127.0.0.1:2181", 5000, this);

            // callback 함수와 같이 호출하면 async 호출이 된다.
            zk.create("/async/test101", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new CreateCallBack() , null);

            // 이 부분은 create 호출 후 서버로부터 응답이 오지 않아도 바로 실행된다.
            System.out.println("do something");

            // 아래 코드가 없으면 테스트 프로그램이 바로 종료되기 때문에 callback이 실행되기 전에는 종료되지 않게 하기 위한 코드
            synchronized(monitor) {
                monitor.wait();
            }
        } catch (Exception e) {
            e .printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {

    }
    public static void main(String[] args) {
        new AsyncAPITest().start();
    }
}
