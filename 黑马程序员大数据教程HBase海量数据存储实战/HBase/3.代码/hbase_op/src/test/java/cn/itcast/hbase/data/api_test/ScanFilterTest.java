package cn.itcast.hbase.data.api_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ScanFilterTest {
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
    public void scanFilterTest() throws IOException {
        // 1.	获取表
        Table table = connection.getTable(TABLE_NAME);

        // 2.	构建scan请求对象
        Scan scan = new Scan();

        // 3.	构建两个过滤器
        // a)	构建两个日期范围过滤器（注意此处请使用RECORD_DATE——抄表日期比较
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("RECORD_DATE")
                , CompareOperator.GREATER_OR_EQUAL
                , new BinaryComparator(Bytes.toBytes("2020-06-01")));

        SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("RECORD_DATE")
                , CompareOperator.LESS_OR_EQUAL
                , new BinaryComparator(Bytes.toBytes("2020-06-30")));

        // b)	构建过滤器列表
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, startFilter, endFilter);

        // 4.	执行scan扫描请求
        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        Iterator<Result> iterator = resultScanner.iterator();

        // 5.	迭代打印result
        while(iterator.hasNext()) {
            Result result = iterator.next();

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

                String value = "";

                // 解决乱码问题：
                // 思路：
                // 如果某个列是以下列中的其中一个，调用toDouble将它认为是一个数值来转换
                //1.	NUM_CURRENT
                //2.	NUM_PREVIOUS
                //3.	NUM_USAGE
                //4.	TOTAL_MONEY
                if(columnName.equals("NUM_CURRENT")
                    || columnName.equals("NUM_PREVIOUS")
                    || columnName.equals("NUM_USAGE")
                    || columnName.equals("TOTAL_MONEY")) {
                    value = Bytes.toDouble(cell.getValueArray()) + "";
                }
                else {
                    // 获取值
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }

                System.out.println(cf + ":" + columnName + " -> " + value);
            }
        }

        // 7.	关闭ResultScanner（这玩意把转换成一个个的类似get的操作，注意要关闭释放资源）
        resultScanner.close();
        // 8.	关闭表
        table.close();

    }

    @AfterTest
    public void afterTest() throws IOException {
        connection.close();
    }
}
