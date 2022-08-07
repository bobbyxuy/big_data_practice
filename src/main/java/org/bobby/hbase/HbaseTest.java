package org.bobby.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTest {
    private static final String COL_FAMILY_INFO = "info";
    private static final String COL_FAMILY_SCORE = "score";

    public static void main(String[] args) throws IOException {
        // 建立连接
        System.out.println("hello");
        Configuration configuration = HBaseConfiguration.create();
        // emr-header-1.cluster-285604
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        System.out.println("start connection");
        Admin admin = conn.getAdmin();
        System.out.println("connection succeed");

        // TODO: name space 没有给
        TableName tableName = TableName.valueOf("xuyingzhe:student");

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            System.out.println("Table creating");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor(COL_FAMILY_INFO));
            hTableDescriptor.addFamily(new HColumnDescriptor(COL_FAMILY_SCORE));
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        insertColumn(conn, tableName, new StudentInfo().name("Tom")
                .studentId("20210000000001")
                .classRoom(1).understanding(75).programming(82));
        insertColumn(conn, tableName, new StudentInfo().name("Jerry")
                .studentId("20210000000002")
                .classRoom(1).understanding(85).programming(67));
        insertColumn(conn, tableName, new StudentInfo().name("Jack")
                .studentId("20210000000003")
                .classRoom(2).understanding(80).programming(80));
        insertColumn(conn, tableName, new StudentInfo().name("Rose")
                .studentId("20210000000004")
                .classRoom(2).understanding(60).programming(61));
        insertColumn(conn, tableName, new StudentInfo().name("XuYingzhe")
                .studentId("G20220735040047")
                .classRoom(3).understanding(100).programming(100));

        System.out.println("Data insert success");

        // 查看数据
        String rowKey = "Tom";
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }

    private static void insertColumn(Connection conn, TableName tableName, StudentInfo studentInfo) throws IOException {
        Put put = new Put(Bytes.toBytes(studentInfo.name));
        put.addColumn(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("student_id"), Bytes.toBytes(studentInfo.studentId));
        put.addColumn(Bytes.toBytes(COL_FAMILY_INFO), Bytes.toBytes("class"), Bytes.toBytes(studentInfo.classRoom));
        put.addColumn(Bytes.toBytes(COL_FAMILY_SCORE), Bytes.toBytes("understanding"), Bytes.toBytes(studentInfo.understanding));
        put.addColumn(Bytes.toBytes(COL_FAMILY_SCORE), Bytes.toBytes("programming"), Bytes.toBytes(studentInfo.programming));
        conn.getTable(tableName).put(put);
    }

    private static class StudentInfo {
        String name;
        String studentId;
        int classRoom;
        int understanding;
        int programming;

        StudentInfo name(String name) {
            this.name = name;
            return this;
        }

        StudentInfo studentId(String studentId) {
            this.studentId = studentId;
            return this;
        }

        StudentInfo classRoom(int room) {
            this.classRoom = room;
            return this;
        }

        StudentInfo understanding(int understanding) {
            this.understanding = understanding;
            return this;
        }

        StudentInfo programming(int programming) {
            this.programming = programming;
            return this;
        }
    }
}
