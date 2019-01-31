package com.victor.controller;

import com.victor.service.WBService;

/**
 * 微博控制器
 */
public class WBController {
    //微博服务对象
    private WBService wbService = new WBService();

    /**
     * 初始化操作
     */
    public void init() {
        try {
            wbService.start();
            //创建命名空间
            wbService.createNameSpace();
            //创建表
            wbService.createTables();
            wbService.end();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publish(String userStar, String content) {
        try {
            wbService.start();
            wbService.publish(userStar,content);
            wbService.end();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void attend(String fanID, String starID) {
        try {
            wbService.start();
            wbService.attend(fanID,starID);
            wbService.end();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void viewContent(String fanID, String starID) {
        try {
            wbService.start();
            wbService.viewContent(fanID,starID);
            wbService.end();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void cancelAttendUser(String fanID, String starID) {
        try {
            wbService.start();
            wbService.cancelAttendUser(fanID,starID);
            wbService.end();
            System.out.println(fanID+" 不再关注 "+starID);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
