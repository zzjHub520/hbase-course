package cn.itcast.hbase.admin.api_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;


public class TableAdminTest {

    private Connection connection;
    private Admin admin;

    @BeforeTest
    public void beforeTest() throws IOException {
        // 1.	使用HbaseConfiguration.create()创建Hbase配置
        Configuration configuration = HBaseConfiguration.create();
        // 2.	使用ConnectionFactory.createConnection()创建Hbase连接
        connection = ConnectionFactory.createConnection(configuration);
        // 3.	要创建表，需要基于Hbase连接获取admin管理对象
        // 要创建表、删除表需要和HMaster连接，所以需要有一个admin对象
        admin = connection.getAdmin();
    }

    @Test
    public void createTableTest() throws IOException {
        TableName tableName = TableName.valueOf("WATER_BILL");

        // 1.	判断表是否存在
        if(admin.tableExists(tableName)) {
            // a)	存在，则退出
            return;
        }

        // 构建表
        // 2.	使用TableDescriptorBuilder.newBuilder构建表描述构建器
        // TableDescriptor: 表描述器，描述这个表有几个列蔟、其他的属性都是在这里可以配置
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        // 3.	使用ColumnFamilyDescriptorBuilder.newBuilder构建列蔟描述构建器
        // 创建列蔟也需要有列蔟的描述器，需要用一个构建起来构建ColumnFamilyDescriptor
        // 经常会使用到一个工具类：Bytes（hbase包下的Bytes工具类）
        // 这个工具类可以将字符串、long、double类型转换成byte[]数组
        // 也可以将byte[]数组转换为指定类型
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C1"));

        // 4.	构建列蔟描述，构建表描述
        ColumnFamilyDescriptor cfDes = columnFamilyDescriptorBuilder.build();

        // 建立表和列蔟的关联
        tableDescriptorBuilder.setColumnFamily(cfDes);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        // 5.	创建表
        admin.createTable(tableDescriptor);
    }

    @Test
    public void deleteTableTest() throws IOException {
        TableName tableName = TableName.valueOf("WATER_BILL");

        // 1.	判断表是否存在
        if(admin.tableExists(tableName)) {
            // 2.如果存在，则禁用表
            admin.disableTable(tableName);
            // 3.再删除表
            admin.deleteTable(tableName);
        }
    }

    @AfterTest
    public void afterTest() throws IOException {
        // 4.	使用admin.close、connection.close关闭连接
        admin.close();
        connection.close();
    }
}
