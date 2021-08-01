package rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;
import java.util.HashMap;

public class MyInterfaceImpl  implements MyInterface {
    HashMap<Long, String> students = new HashMap<Long, String>();
    public MyInterfaceImpl() {
       students.put(20210123456789L,"心心");

    }
     //实现加法
     @Override
     public int add(int number1, int number2) {
         System.out.println("number1 = " + number1 + " number2 = " + number2);
         return number1 + number2;
     }


    @Override
    public String getName(long stuNumber) {
        return  students.get(stuNumber);
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}