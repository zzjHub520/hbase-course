package cn.itcast.momo_chat.service.impl;

import cn.itcast.momo_chat.entity.Msg;
import cn.itcast.momo_chat.service.ChatMessageService;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class PhoenixChatMessageService implements ChatMessageService {

    private Connection connection;

    public PhoenixChatMessageService() throws Exception {
        // 1. 加载驱动
        Class.forName(PhoenixDriver.class.getName());
        // 2. 获取JDBC连接
        connection = DriverManager.getConnection("jdbc:phoenix:node1.itcast.cn:2181");
    }

    @Override
    public List<Msg> getMessage(String date, String sender, String receiver) throws Exception {
        // 1. SQL语句
        String sql = "select * from \"MOMO_CHAT\".\"MSG\" where substr(\"msg_time\", 0, 10) = ? and \"sender_account\" = ? and \"receiver_account\" = ?";

        // 2. 构建一个prepareStatement
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        // 3. 设置Prestatement对应的参数
        preparedStatement.setString(1, date);
        preparedStatement.setString(2, sender);
        preparedStatement.setString(3, receiver);

        // 4. 执行SQL语句，获取到一个ResultSet
        ResultSet resultSet = preparedStatement.executeQuery();

        ArrayList<Msg> msgList = new ArrayList<>();

        // 5. 迭代ResultSet将数据封装在Msg里面
        while(resultSet.next()) {
            Msg msg = new Msg();
            msg.setMsg_time(resultSet.getString("msg_time"));
            msg.setSender_nickyname(resultSet.getString("sender_nickyname"));
            msg.setSender_account(resultSet.getString("sender_account"));
            msg.setSender_sex(resultSet.getString("sender_sex"));
            msg.setSender_ip(resultSet.getString("sender_ip"));
            msg.setSender_os(resultSet.getString("sender_os"));
            msg.setSender_phone_type(resultSet.getString("sender_phone_type"));
            msg.setSender_network(resultSet.getString("sender_network"));
            msg.setSender_gps(resultSet.getString("sender_gps"));
            msg.setReceiver_nickyname(resultSet.getString("receiver_nickyname"));
            msg.setReceiver_ip(resultSet.getString("receiver_ip"));
            msg.setReceiver_account(resultSet.getString("receiver_account"));
            msg.setReceiver_os(resultSet.getString("receiver_os"));
            msg.setReceiver_phone_type(resultSet.getString("receiver_phone_type"));
            msg.setReceiver_network(resultSet.getString("receiver_network"));
            msg.setReceiver_gps(resultSet.getString("receiver_gps"));
            msg.setReceiver_sex(resultSet.getString("receiver_sex"));
            msg.setMsg_type(resultSet.getString("msg_type"));
            msg.setDistance(resultSet.getString("distance"));
            msg.setMessage(resultSet.getString("message"));

            msgList.add(msg);
        }

        // 关闭资源
        resultSet.close();
        preparedStatement.close();

        return msgList;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
