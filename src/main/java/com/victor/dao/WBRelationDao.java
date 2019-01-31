package com.victor.dao;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class WBRelationDao {
    public void createTable(Connection conn) throws Exception {

        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf("hadoop:relation");

        //如果表已经存在，则删除
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("hadoop:relation"));
        //增加粉丝列族
        HColumnDescriptor hColumnFanDescriptor = new HColumnDescriptor("fans");
        hTableDescriptor.addFamily(hColumnFanDescriptor);
        //增加关注列族
        HColumnDescriptor hColumnAttendDescriptor = new HColumnDescriptor("attend");
        hTableDescriptor.addFamily(hColumnAttendDescriptor);


        admin.createTable(hTableDescriptor);

        //关闭admin
        admin.close();
    }

    public List<String> getFanIDs(Connection conn, String starID) throws Exception {
        List<String> fanIDs = new ArrayList<String>();

        TableName tableName = TableName.valueOf("hadoop:relation");
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(starID));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            fanIDs.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
        }

        table.close();
        return fanIDs;
    }

    public void insertAttendData(Connection conn, String fanID, String starID) throws Exception {
        TableName tableName = TableName.valueOf("hadoop:relation");
        Table table = conn.getTable(tableName);

        //粉丝的关注中增加一条
        Put putAttend = new Put(Bytes.toBytes(fanID));
        byte[] attendFamily = Bytes.toBytes("attend");
        byte[] attendColumn = Bytes.toBytes(starID);
        byte[] attendValue = Bytes.toBytes(starID);
        putAttend.addColumn(attendFamily, attendColumn, attendValue);
        table.put(putAttend);

        table.close();
    }

    public void insertFanData(Connection conn, String fanID, String starID) throws Exception {
        TableName tableName = TableName.valueOf("hadoop:relation");
        Table table = conn.getTable(tableName);

        //明星的粉丝里增加一条
        Put putFan = new Put(Bytes.toBytes(starID));
        byte[] fanFamily = Bytes.toBytes("fans");
        byte[] fanColumn = Bytes.toBytes(fanID);
        byte[] fanValue = Bytes.toBytes(fanID);
        putFan.addColumn(fanFamily, fanColumn, fanValue);
        table.put(putFan);

        //关闭表
        table.close();

    }


    public void cancelAttend(Connection conn, String fanID, String starID) throws Exception {
        TableName tableName = TableName.valueOf("hadoop:relation");
        Table table = conn.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes(fanID));
        byte[] column = Bytes.toBytes(starID);
        byte[] family = Bytes.toBytes("attend");
        delete.addColumn(family, column);

        table.delete(delete);
        table.close();
    }

    public void cancelFan(Connection conn, String starID, String fanID) throws Exception{
        TableName tableName = TableName.valueOf("hadoop:relation");
        Table table = conn.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes(starID));
        byte[] column = Bytes.toBytes(fanID);
        byte[] family = Bytes.toBytes("fans");
        delete.addColumn(family, column);

        table.delete(delete);
        table.close();
    }
}
