package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Client {
    public static void main(String[] args) {
        try {
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L, new InetSocketAddress("127.0.0.1", 8082), new Configuration());
         //   int res = proxy.add(1, 2);
         //   System.out.println(res);
            String name = proxy.getName(202100000000000L);
            System.out.println("student name of student number 20210000000000 is " + name);
            String name1 = proxy.getName(20210123456789L);
            System.out.println("student name of student number 20210123456789 is " + name1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
