package com.victor.controller;

import com.victor.service.WBService;

import java.io.IOException;

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
}
