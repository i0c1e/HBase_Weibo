package com.victor.dao;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class WBInboxDao {
    public void createTable(Connection conn) throws Exception {
        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf("hadoop:inbox");
        //如果表已经存在，则删除
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("hadoop:inbox"));
        //增加列族
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        hTableDescriptor.addFamily(hColumnDescriptor);
        //设置5个版本，保留5条消息
        hColumnDescriptor.setMaxVersions(5);
        hColumnDescriptor.setMinVersions(5);
        admin.createTable(hTableDescriptor);

        //关闭admin
        admin.close();
    }

    public void insertMessage(Connection conn, String starID, List<String> fanID, String contentRK) throws Exception {
        TableName tableName = TableName.valueOf("hadoop:inbox");
        Table table = conn.getTable(tableName);

        List<Put> puts = new ArrayList<Put>();
        for (String fan : fanID) {
            Put put = new Put(Bytes.toBytes(contentRK));

            byte[] family = Bytes.toBytes("info");
            byte[] column = Bytes.toBytes(starID);
            byte[] value = Bytes.toBytes(fan);
            put.addColumn(family, column, value);

            puts.add(put);
        }
        table.put(puts);

        table.close();
    }

    /**
     * 向粉丝的收件箱里推送明星的消息
     *
     * @param conn
     * @param starID
     * @param fanID
     * @param weiboRK
     */
    public void pushMessage(Connection conn, String starID, String fanID,
                            List<String> weiboRK) throws Exception {
        TableName inboxTableName = TableName.valueOf("hadoop:inbox");
        Table inboxTable = conn.getTable(inboxTableName);
        TableName weiboTableName = TableName.valueOf("hadoop:weibo");
        Table weiboTable = conn.getTable(weiboTableName);

        int count=0;

        for (String rk : weiboRK) {
            Get get = new Get(Bytes.toBytes(rk));
            Result result = weiboTable.get(get);

            Put put = new Put(Bytes.toBytes(fanID));

            byte[] column = Bytes.toBytes("content");
            byte[] family = Bytes.toBytes("info");
            byte[] star = Bytes.toBytes(starID);
            byte[] value = result.getValue(family, column);
//            put.addColumn(family, star, value);

            long ts = System.currentTimeMillis()+ (++count);
            put.addColumn(family,star,ts,value);

            inboxTable.put(put);
        }

        inboxTable.close();
        weiboTable.close();
    }

    public void deletePush(Connection conn, String fanID, String starID)throws Exception {
        TableName tableName = TableName.valueOf("hadoop:inbox");
        Table table = conn.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes(fanID));
        byte[] family = Bytes.toBytes("info");
        byte[] column = Bytes.toBytes(starID);
        delete.addColumns(family,column);

        table.delete(delete);

        table.close();
    }
}
