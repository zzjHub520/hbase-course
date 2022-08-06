package cn.itcast.hbase.data.api_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class DataOpTest {

    // Connection是一个重量级的对象，不能频繁去创建Connection
    // Connection是线程安全的
    private Connection connection;
    private TableName TABLE_NAME = TableName.valueOf("WATER_BILL");

    @BeforeTest
    public void beforeTest() throws IOException {
        // 1.	使用HbaseConfiguration.create()创建Hbase配置
        Configuration configuration = HBaseConfiguration.create();
        // 2.	使用ConnectionFactory.createConnection()创建Hbase连接
        connection = ConnectionFactory.createConnection(configuration);
    }

    @Test
    public void putTest() throws IOException {
        // 1.	使用Hbase连接获取Htable
        Table table = connection.getTable(TABLE_NAME);

        // 2.	构建ROWKEY、列蔟名、列名
        String rowkey = "4944191";
        String columnFamily = "C1";
        String columnName = "NAME";
        String columnNameADDRESS = "ADDRESS";
        String columnNameSEX = "SEX";
        String columnNamePAY_DATE = "PAY_DATE";
        String columnNameNUM_CURRENT = "NUM_CURRENT";
        String columnNameNUM_PREVIOUS = "NUM_PREVIOUS";
        String columnNameNUM_USAGE = "NUM_USAGE";
        String columnNameTOTAL_MONEY = "TOTAL_MONEY";
        String columnNameRECORD_DATE = "RECORD_DATE";
        String columnNameLATEST_DATE = "LATEST_DATE";

        // value:

        // 3.	构建Put对象（对应put命令）
        Put put = new Put(Bytes.toBytes(rowkey));

        // 4.	添加姓名列
        // 使用alt + 鼠标左键列编辑，按住ctrl + shift + 左箭头/右箭头选择单词
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes("登卫红"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameADDRESS),Bytes.toBytes("贵州省铜仁市德江县7单元267室"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameSEX),Bytes.toBytes("男"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNamePAY_DATE),Bytes.toBytes("2020-05-10"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameNUM_CURRENT),Bytes.toBytes("308.1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameNUM_PREVIOUS),Bytes.toBytes("283.1"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameNUM_USAGE),Bytes.toBytes("25"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameTOTAL_MONEY),Bytes.toBytes("150"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameRECORD_DATE),Bytes.toBytes("2020-04-25"));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNameLATEST_DATE),Bytes.toBytes("2020-06-09"));

        // 5.	使用Htable表对象执行put操作
        table.put(put);

        // 6.	关闭Htable表对象
        // HTable是一个轻量级的对象，可以经常创建
        // HTable它是一个非线程安全的API
        table.close();

    }

    @Test
    public void getTest() throws IOException {
        // 1.	获取HTable
        Table table = connection.getTable(TABLE_NAME);

        // 2.	使用rowkey构建Get对象
        Get get = new Get(Bytes.toBytes("4944191"));

        // 3.	执行get请求
        Result result = table.get(get);

        // 4.	获取所有单元格
        // 列出所有的单元格
        List<Cell> cellList = result.listCells();

        // 5.	打印rowkey
        byte[] rowkey = result.getRow();
        System.out.println(Bytes.toString(rowkey));
        // 6.	迭代单元格列表
        for (Cell cell : cellList) {
            // 将字节数组转换为字符串
            // 获取列蔟的名称
            String cf = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            // 获取列的名称
            String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            // 获取值
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(cf + ":" + columnName + " -> " + value);
        }
        // 7.	关闭表
        table.close();
    }

    @Test
    public void deleteTest() throws IOException {
        // 1.	获取HTable对象
        Table table = connection.getTable(TABLE_NAME);

        // 2.	根据rowkey构建delete对象
        Delete delete = new Delete(Bytes.toBytes("4944191"));

        // 3.	执行delete请求
        table.delete(delete);

        // 4.	关闭表
        table.close();
    }

    @AfterTest
    public void afterTest() throws IOException {
        connection.close();
    }
}
