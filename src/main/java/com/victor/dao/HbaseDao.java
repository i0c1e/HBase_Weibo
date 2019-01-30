package com.victor.dao;

import com.victor.controller.WBController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseDao {

    //获取连接对象
    public Connection getConnection() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        return conn;

    }

    public boolean getNamespace(Connection conn) throws Exception {
        try {
            Admin admin = conn.getAdmin();
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor("hadoop");
            return true;
        } catch (NamespaceNotFoundException e) {
            e.printStackTrace();
            return false;
        }

    }

    public void createNamespace(Connection conn) throws Exception {
        Admin admin = conn.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("hadoop").build();
        admin.createNamespace(namespaceDescriptor);
    }
}
