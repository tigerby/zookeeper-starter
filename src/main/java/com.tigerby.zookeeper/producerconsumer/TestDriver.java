package com.tigerby.zookeeper.producerconsumer;

/**
 * use example:
 *
 * java TestDriver tiger01:2181,tiger02:2181,tiger03:2181 2 3
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public class TestDriver {
    public static String _zkServers;

    static class ProducerThread extends Thread {
        Producer producer;

        public ProducerThread(String id) {
            producer = new Producer(_zkServers, id);
        }

        @Override
        public void run() {
            producer.startProducer();
        }
    }

    static class ConsumerThread extends Thread {
        Consumer consumer;

        public ConsumerThread(String id) {
            consumer = new Consumer(_zkServers, id);
        }

        @Override
        public void run() {
            consumer.startConsumer();
        }
    }

    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("Usage: java TestDriver ZK_SERVERS NUMBER_OF_PRODUCER NUMBER_OF_CONSUMER");
            System.exit(0);
        }

        String zkServers = args[0];
        String numberOfProducer = args[1];
        String numberOfConsumer = args[2];

        _zkServers = zkServers;

        for(int i=0; i< Integer.parseInt(numberOfConsumer); i++) {
            new ConsumerThread("[CONSUMER-" + (i + 1) + "]").start();
        }

        for(int i=0; i< Integer.parseInt(numberOfProducer); i++) {
            new ProducerThread("[PRODUCER-" + (i + 1) + "]").start();
        }
    }
}
