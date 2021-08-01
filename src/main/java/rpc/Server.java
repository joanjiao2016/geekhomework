package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class Server {
     public static void main(String[] args) {
            RPC.Builder builder = new RPC.Builder(new Configuration());
           //服务器Ip 地址
            builder.setBindAddress("127.0.0.1");
            //端口号
            builder.setPort(8082);

            builder.setProtocol(MyInterface.class);
            builder.setInstance(new MyInterfaceImpl());

            try {
                RPC.Server server = builder.build();
                server.start();
                System.out.println("The server started ...");
               } catch (IOException e) {
                   e.printStackTrace();
               }

        }

}
