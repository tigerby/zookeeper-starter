package com.tigerby.thrift;

import org.apache.thrift.TException;

/**
 * THIS IS TEMPORARY. ONLY TENDS TO NO COMPILE ERROR.
 */
public class HelloService {
    public interface Iface {
        public String greeting(String name, int age) throws TException;
    }

    public class Processor {
        public Processor(Iface iface) {

        }
    }

    public static class Client implements Iface {

        @Override
        public String greeting(String name, int age) throws TException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
