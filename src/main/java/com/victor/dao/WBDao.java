package com.victor.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class WBDao {

    //获取连接对象
    public Connection getConnection() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        return conn;

    }

    public boolean hasNamespace(Connection conn) throws Exception {
        Admin admin = conn.getAdmin();

        try {
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor("hadoop");
            return true;

        } catch (NamespaceNotFoundException e) {
            return false;
        } finally {
            admin.close();
        }

    }


    public void createNamespace(Connection conn) throws Exception {
        Admin admin = conn.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("hadoop").build();
        admin.createNamespace(namespaceDescriptor);
    }

    public void createTable(Connection conn) throws Exception {
        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf("hadoop:weibo");
        //如果表已经存在，则删除
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("hadoop:weibo"));
        //增加列族
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);

        //关闭admin
        admin.close();
    }

    public String insertData(Connection conn, String starID, Long time, String content) throws Exception {
        TableName tableName = TableName.valueOf("hadoop:weibo");
        Table table = conn.getTable(tableName);


        //对时间处理，实现rowkey按照时间戳倒排
        byte[] rowkey = Bytes.toBytes(starID + "-" + (Long.MAX_VALUE - time));
        byte[] family = Bytes.toBytes("info");
        byte[] column = Bytes.toBytes("content");
        byte[] value = Bytes.toBytes(content);

        Put put = new Put(rowkey);
        put.addColumn(family, column, time, value);

        table.put(put);
        table.close();

        return Bytes.toString(rowkey);
    }

    public List<String> scanData(Connection conn, String starID) throws Exception {
        List<String> weiboRK = new ArrayList<String>();
        TableName tableName = TableName.valueOf("hadoop:weibo");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(starID + "-"));
        scan.setStopRow(Bytes.toBytes(starID + "-|"));
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            byte[] rowkey = result.getRow();
            weiboRK.add(Bytes.toString(rowkey));

            if (weiboRK.size() >= 5) break;
        }

        table.close();
        return weiboRK;
    }

    public List<String> getAllRow(Connection conn, String starID) throws Exception {
        List<String> allrow = new ArrayList<String>();
        TableName tableName = TableName.valueOf("hadoop:weibo");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(starID + "-"));
        scan.setStopRow(Bytes.toBytes(starID + "-|"));
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            byte[] rowkey = result.getRow();
            allrow.add(Bytes.toString(rowkey));

        }
        table.close();
        return allrow;
    }

    public List<String> getDetail(Connection conn, List<String> allrow) throws Exception {
        List<String> details = new ArrayList<String>();
        TableName tableName = TableName.valueOf("hadoop:weibo");
        Table table = conn.getTable(tableName);

        for (String rk : allrow) {

            Get get = new Get(Bytes.toBytes(rk));
            Result result = table.get(get);
            byte[] family = Bytes.toBytes("info");
            byte[] column = Bytes.toBytes("content");
            byte[] value = result.getValue(family, column);
            details.add(Bytes.toString(value));
        }

        return details;
    }

}
