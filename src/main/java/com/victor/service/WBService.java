package com.victor.service;

import com.victor.dao.WBDao;
import com.victor.dao.WBInboxDao;
import com.victor.dao.WBRelationDao;
import org.apache.hadoop.hbase.client.*;

import java.util.List;


public class WBService {

    //微博数据访问对象
    private WBDao wbDao = new WBDao();
    private WBInboxDao wbInboxDao = new WBInboxDao();
    private WBRelationDao wbRelationDao = new WBRelationDao();

    Connection conn = null;

    ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

    public synchronized void start() throws Exception {
        conn = connHolder.get();
        if (conn == null) {
            Connection conn = wbDao.getConnection();
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

        conn = connHolder.get();
        //判断命名空间是否存在
        boolean flag = wbDao.hasNamespace(conn);

        if (!flag) {
            //如果命名空间不存在，创建新的命名空间
            wbDao.createNamespace(conn);
        }


    }

    public void createTables() throws Exception {

        conn = connHolder.get();

        wbDao.createTable(conn);
        wbInboxDao.createTable(conn);
        wbRelationDao.createTable(conn);


    }

    public void publish(String starID, String content) throws Exception {
        Connection conn = connHolder.get();

        //向微博表插入数据
        Long time = System.currentTimeMillis();
        String contentRK = wbDao.insertData(conn, starID, time, content);

        //获取粉丝ID
        List<String> fanIDS = wbRelationDao.getFanIDs(conn, starID);

        //向粉丝收件箱推送消息
        wbInboxDao.insertMessage(conn, starID, fanIDS, contentRK);
    }

    public void attend(String fanID, String starID) throws Exception{
        conn = connHolder.get();

        //粉丝关注明星
        wbRelationDao.insertAttendData(conn,fanID,starID);
        //明星增加粉丝
        wbRelationDao.insertFanData(conn,fanID,starID);
        //获取明星的微博信息
        List<String> weiboRK = wbDao.scanData(conn,starID);
        //向粉丝的收件箱里推送明星的数据
        if (!weiboRK.isEmpty()){
            wbInboxDao.pushMessage(conn,starID,fanID,weiboRK);
        }
    }

    public void viewContent(String fanID, String starID) throws Exception{
        conn = connHolder.get();

        //获取明星发过的所有微博rowkey
        List<String> allrow = wbDao.getAllRow(conn,starID);

        //获取所有发过微博的详细内容
        List<String> details = wbDao.getDetail(conn,allrow);

        for (String detail : details) {
            System.out.println(starID+" : "+detail);
        }
    }

    public void cancelAttendUser(String fanID, String starID) throws Exception{
        conn = connHolder.get();

        //取消粉丝的关注列表
        wbRelationDao.cancelAttend(conn,fanID,starID);
        //取消明星的粉丝列表
        wbRelationDao.cancelFan(conn,starID,fanID);
        //删掉粉丝收件箱中相关明星的消息
        wbInboxDao.deletePush(conn,fanID,starID);
    }
}
