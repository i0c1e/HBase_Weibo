import com.victor.controller.WBController;

public class TestWB {
    public static void main(String[] args) {
        //测试微博功能

        //搭建项目架构

        //三层架构(Controller,Service,Dao)
        //Controller: 控制层, 调度作用
        //Service:服务层，处理业务逻辑
        //数据访问层，处理数据操作
        WBController wbController = new WBController();
        //控制器初始化
        wbController.init();


    }
}
