package cn.itcast.momo_chat.service.impl;

import cn.itcast.momo_chat.entity.Msg;
import cn.itcast.momo_chat.service.ChatMessageService;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 使用HBase原生API来实现消息的查询
 */
public class HBaseNativeChatMessageService implements ChatMessageService {

    private Connection connection;
    private SimpleDateFormat simpleDateFormat;

    public HBaseNativeChatMessageService() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
    }

    /**
     *
     * @param date 日期 2020-09-10
     * @param sender 发件人
     * @param receiver 收件人
     * @return
     * @throws Exception
     */
    @Override
    public List<Msg> getMessage(String date, String sender, String receiver) throws Exception {
        // 1.	构建scan对象
        Scan scan = new Scan();

        // 构建两个带时分秒的日期字符串
        String startDateStr = date + " 00:00:00";
        String endDateStr = date + " 23:59:59";

        // 2.	构建用于查询时间的范围，例如：2020-10-05 00:00:00 – 2020-10-05 23:59:59
        // 3.	构建查询日期的两个Filter，大于等于、小于等于，此处过滤单个列使用SingleColumnValueFilter即可。
        SingleColumnValueFilter startDateFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("msg_time")
                , CompareOperator.GREATER_OR_EQUAL
                , new BinaryComparator(Bytes.toBytes(startDateStr)));

        SingleColumnValueFilter endDateFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("msg_time")
                , CompareOperator.LESS_OR_EQUAL
                , new BinaryComparator(Bytes.toBytes(endDateStr)));

        // 4.	构建发件人Filter
        SingleColumnValueFilter senderFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("sender_account")
                , CompareOperator.EQUAL
                , new BinaryComparator(Bytes.toBytes(sender)));

        // 5.	构建收件人Filter
        SingleColumnValueFilter receiverFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("receiver_account")
                , CompareOperator.EQUAL
                , new BinaryComparator(Bytes.toBytes(receiver)));

        // 6.	使用FilterList组合所有Filter
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL
                , startDateFilter
                , endDateFilter
                , senderFilter
                , receiverFilter);

        // 7.	设置scan对象filter
        scan.setFilter(filterList);

        // 8.	获取HTable对象，并调用getScanner执行
        Table table = connection.getTable(TableName.valueOf("MOMO_CHAT:MSG"));
        ResultScanner resultScanner = table.getScanner(scan);

        // 9.	获取迭代器，迭代每一行，同时迭代每一个单元格
        Iterator<Result> iterator = resultScanner.iterator();

        // 创建一个列表，用于保存查询出来的消息
        ArrayList<Msg> msgList = new ArrayList<>();

        while(iterator.hasNext()) {
            // 每一行查询出来的数据都是一个Msg对象
            Result result = iterator.next();
            Msg msg = new Msg();
            // 获取rowkey
            String rowkey = Bytes.toString(result.getRow());
            // 单元格列表
            List<Cell> cellList = result.listCells();

            for (Cell cell : cellList) {
                // 根据当前的cell单元格的列名来判断，设置对应的字段
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                if(columnName.equals("msg_time")) {
                    msg.setMsg_time(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_nickyname")){
                    msg.setSender_nickyname(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_account")){
                    msg.setSender_account(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_sex")){
                    msg.setSender_sex(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_ip")){
                    msg.setSender_ip(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_os")){
                    msg.setSender_os(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_phone_type")){
                    msg.setSender_phone_type(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_network")){
                    msg.setSender_network(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("sender_gps")){
                    msg.setSender_gps(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_nickyname")){
                    msg.setReceiver_nickyname(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_ip")){
                    msg.setReceiver_ip(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_account")){
                    msg.setReceiver_account(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_os")){
                    msg.setReceiver_os(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_phone_type")){
                    msg.setReceiver_phone_type(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_network")){
                    msg.setReceiver_network(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_gps")){
                    msg.setReceiver_gps(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("receiver_sex")){
                    msg.setReceiver_sex(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("msg_type")){
                    msg.setMsg_type(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("distance")){
                    msg.setDistance(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equals("message")){
                    msg.setMessage(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
            }

            msgList.add(msg);
        }

        // 关闭资源
        resultScanner.close();
        table.close();

        return msgList;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
