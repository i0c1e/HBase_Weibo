package com.victor.service;

import com.victor.dao.WBContentDao;
import com.victor.dao.HbaseDao;
import com.victor.dao.WBInboxDao;
import com.victor.dao.WBRelationDao;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;


public class WBService {

    //微博数据访问对象
    private HbaseDao hbaseDao = new HbaseDao();
    private WBContentDao wbController = new WBContentDao();
    private WBInboxDao wbInboxDao = new WBInboxDao();
    private WBRelationDao wbRelationDao = new WBRelationDao();

    Connection conn = null;

    ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    public synchronized void start() throws Exception {
        conn = connHolder.get();
        if (conn ==null ){
            Connection conn = hbaseDao.getConnection();
            connHolder.set(conn);
        }
    }

    public void end() throws Exception {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }

    public void createNameSpace() throws Exception {

        conn=connHolder.get();
        //判断命名空间是否存在
        boolean flag = hbaseDao.getNamespace(conn);

        if (!flag) {
            //如果命名空间不存在，创建新的命名空间
            hbaseDao.createNamespace(conn);
        }


    }

    public void createTables() throws Exception {
        conn=connHolder.get();
        //判断表是否存在
        boolean flag = hbaseDao.getNamespace(conn);

        if (!flag) {
            //如果命名空间不存在，创建新的命名空间
            hbaseDao.createNamespace(conn);
        }

    }
}
