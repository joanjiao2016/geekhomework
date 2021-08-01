package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HBaseOp {
    Configuration conf = HBaseConfiguration.create();
    org.apache.hadoop.hbase.client.Connection connection;

    public Connection getConnection() {
        try {
            //如果不存在则创建
            if (connection == null) {
                //将hbase-site.xml 文件放入项目的resources目录中可以方便获取connection
                connection = ConnectionFactory.createConnection(conf);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return this.connection;

    }

    public void createTable(Connection connection,String name) {
        System.out.println("开始创建表：" + name+" ...");
        TableName tableName = TableName.valueOf(name);
        Admin admin = null;

        try {
            admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                // 创建表描述符
                HTableDescriptor descriptor = new HTableDescriptor(tableName);

                // 创建列族描述符
                HColumnDescriptor family1 = new HColumnDescriptor("info");
                descriptor.addFamily(family1);
                HColumnDescriptor family2 = new HColumnDescriptor("score");
                descriptor.addFamily(family2);

                //createTable
                admin.createTable(descriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("创建表：" + name+" 成功！\n");
    }

    public void putData(Connection connection,String tableName, String rowKey, String studentId, String classId, String understanding, String programming) {
        System.out.println("插入行数据:"+rowKey+"|"+studentId+"|"+classId+"|"+understanding+"|"+programming);
        HTable hTable = null;
        try {
            hTable = (HTable) connection.getTable(TableName.valueOf(tableName));


            Put row = new Put(Bytes.toBytes(rowKey));
            //向Put对象中组装数据
            row.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes(studentId));
            row.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes(classId));

            row.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes(understanding));
            row.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes(programming));

            hTable.put(row);
            hTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("插入数据成功！\n");
    }

    //获取指定rowKey的行信息
    public void getData(Connection connection,String tableName, String rowKey) {

        try {
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            //get.setMaxVersions();显示所有版本
            //get.setTimeStamp();显示指定时间戳的版本
            Result result = table.get(get);
            System.out.println("获取表中一行信息如下：");
            for (Cell cell : result.rawCells()) {
                System.out.println("行键:" + Bytes.toString(result.getRow()));
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("时间戳:" + cell.getTimestamp());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    //table scan
    public  void getAllRows(Connection connection,String tableName){
        System.out.println("表数据信息如下：");
        try {
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            //得到用于扫描region的对象
            Scan scan = new Scan();
            //使用HTable得到resultcanner实现类的对象
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                Cell[] cells = result.rawCells();
                for(Cell cell : cells){
                    System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)) + " ,列族" + Bytes.toString(CellUtil.cloneFamily(cell)) + ", 列:" + Bytes.toString(CellUtil.cloneQualifier(cell))+" ,值:" + Bytes.toString(CellUtil.cloneValue(cell)));
//                    //得到列族
//                    System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
//                    System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//                    System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

        }catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("查询表数据结束.\n\n");
    }

    //删除指定rowkey的数据
    public void deleteData(Connection connection,String tableName,String rowKey) {
        System.out.println("开始删除数据,rowKey="+rowKey);
        try {
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            table.delete(del);

        }catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("删除数据成功！\n");

    }



    //main函数，测试各个函数的功能
    public static void main(String[] args) {
        HBaseOp hbaseTest = new HBaseOp();
        Connection con = hbaseTest.getConnection();
        //创建表：
        hbaseTest.createTable(con,"jiaoyuan:student" );

        //插入两行数据
        hbaseTest.putData(con,"jiaoyuan:student","jiaoyuantest","G20210735010403","2","90","90");
        hbaseTest.putData(con,"jiaoyuan:student","jiaoyuan","G20210735010403","2","100","100");

        //查询插入后的数据
        hbaseTest.getAllRows(con,"jiaoyuan:student");

        //删除一行数据
        hbaseTest.deleteData(con,"jiaoyuan:student","jiaoyuantest");

        //查询删除后的数据
        hbaseTest.getAllRows(con,"jiaoyuan:student");
    }

}
