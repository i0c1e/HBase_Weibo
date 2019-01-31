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

        //发送微博
        String starID="zhangsan";
        String fanID="lisi";
        String content="Hello World";
        String content2="Hello World !!!";

        wbController.publish(starID,content+"1");
        wbController.publish(starID,content+"2");
        wbController.publish(starID,content+"3");
        wbController.publish(starID,content+"4");
        wbController.publish(starID,content+"5");
        wbController.publish(starID,content+"6");
        wbController.publish(starID,content+"7");
        wbController.publish(starID,content+"8");
        wbController.publish("Alin","welcome to my channel");
        wbController.publish("Alin","it's a good night");
//        wbController.publish(starID,content);
//        wbController.publish(starID,content2);

        //关注用户
        wbController.attend(fanID,starID);
        wbController.attend(fanID,"Alin");


        //查看微博
        wbController.viewContent(fanID,starID);

        //取消关注
        wbController.cancelAttendUser(fanID,starID);

    }
}
