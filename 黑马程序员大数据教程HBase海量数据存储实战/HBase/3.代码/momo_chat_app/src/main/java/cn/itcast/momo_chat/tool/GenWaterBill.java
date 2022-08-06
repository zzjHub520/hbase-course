package cn.itcast.momo_chat.tool;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GenWaterBill {

        public static String[] NAME_LIST = new String[] {"登卫红","茹喜兰","荆秀荣","无玉梅","毋文芬","己爱梅","骆秀珍","天建军","谯振彬","修宏强","念素芬","墨纪保","郏春花","华素真","检喜云","区胜利","北玉香","典洪海","允建英","纵红卫","蛮飞翔","嘉翠翠","俎永力","丰天恩","桓国娟","乐贝贝","遇沁仪","禽柯福","方浩轩","贺艺硕","在正汉","衅芳妤","独照涵","荆子沐","续浩迪","令狐胜龙"};
        public static String[] ADDR_LIST = new String[] {"贵州省铜仁市德江县7单元267室","江西省新余市分宜县钤山镇6单元251室","上海市上海市闸北区共和新路街道7单元192室","陕西省咸阳市武功县小村镇2单元168室","辽宁省本溪市南芬区南芬街道18单元97室","甘肃省武威市民勤县东坝镇15单元69室","河南省周口市太康县马头镇12单元280室","安徽省宣城市市辖区13单元187室","青海省黄南藏族自治州泽库县和日乡8单元185室","湖南省郴州市桂阳县浩塘镇7单元238室","吉林省四平市公主岭市公主岭市种猪场17单元19室","甘肃省嘉峪关市市辖区12单元178室","四川省攀枝花市仁和区务本乡1单元151室","四川省达州市达川区6单元99室","山西省忻州市偏关县新关镇7单元124室","陕西省宝鸡市眉县槐芽镇18单元139室","甘肃省陇南市两当县左家乡20单元65室","湖南省岳阳市湘阴县新泉镇14单元75室","湖北省襄阳市老河口市林茂山林场16单元202室","海南省省直辖县级行政区划定安县13单元144室","青海省海东市民和回族土族自治县18单元52室","黑龙江省绥化市青冈县芦河镇16单元202室","海南省三亚市崖州区20单元118室","内蒙古自治区通辽市奈曼旗新镇16单元161室","湖北省随州市市辖区5单元285室","辽宁省锦州市市辖区16单元145室","上海市上海市金山区石化街道19单元39室","河南省驻马店市市辖区12单元262室","山西省晋城市泽州县南村镇11单元73室","宁夏回族自治区银川市金凤区长城中路街道8单元145室","青海省海西蒙古族藏族自治州都兰县巴隆乡5单元142室","四川省绵阳市市辖区1单元162室","黑龙江省鹤岗市兴安区红旗镇8单元269室","广西壮族自治区桂林市七星区七星区街道12单元231室","四川省内江市市中区沱江乡3单元245室","海南省三沙市西沙群岛永兴岛18单元87室"};
        public static String[] SEX_LIST = new String[] {"男", "女"};
        public static String[] PAY_DATE_LIST = new String[] {"2020-05-10","2019-06-24","2019-08-11","2019-04-30","2019-02-02","2019-01-27","2019-05-19","2020-10-23","2019-06-03","2020-03-31","2020-09-09","2019-11-19","2020-03-23","2020-07-03","2020-03-19","2020-10-22","2020-05-14","2019-02-03","2020-12-17","2019-02-26","2020-08-21","2019-08-02","2020-09-10","2020-10-25","2020-09-26","2020-10-22","2020-05-14","2020-12-17","2019-02-26","2019-08-02","2020-10-25","2020-05-14","2020-09-09","2019-11-19","2020-07-03","2019-11-19"};
        public static Double[] NUM_CURRENT_LIST = new Double[] {308.1,62.5,255.8,406.2,274.8,64.2,477.7,274.9,391.1,179.8,105.2,271.2,475.9,439.9,185.4,293.5,131.9,445.5,108D,382.2,118.4,304.3,362.7,235.8,369.7,69.9,497D,417.5,398.5,441.7,431.7,339D,421.8,412.4,313.7,430.6};
        public static Double[] NUM_PRE_LIST = new Double[]{283.1,40.5,237.8,379.2,252.8,46.2,457.7,246.9,369.1,159.8,89.2,250.2,452.9,418.9,159.4,272.5,114.9,427.5,78D,356.2,99.4,285.3,341.7,213.8,348.7,45.9,471D,391.5,379.5,422.7,414.7,309D,400.8,388.4,289.7,407.6};
        public static Double[] USAGE_LIST = new Double[]{25D,22D,18D,27D,22D,18D,20D,28D,22D,20D,16D,21D,23D,21D,26D,21D,17D,18D,30D,26D,19D,19D,21D,22D,21D,24D,26D,26D,19D,19D,17D,30D,21D,24D,24D,23D,};
        public static Double[] TOTAL_MONEY = new Double[] {150D,132D,108D,162D,132D,108D,120D,168D,132D,120D,96D,126D,138D,126D,156D,126D,102D,108D,180D,156D,114D,114D,126D,132D,126D,144D,156D,156D,114D,114D,102D,180D,126D,144D,144D,138D,};
        public static String[] RECORD_DATE = new String[] {"2020-04-25","2019-06-09","2019-07-27","2019-04-15","2019-01-18","2019-01-12","2019-05-04","2020-10-08","2019-05-19","2020-03-16","2020-08-25","2019-11-04","2020-03-08","2020-06-18","2020-03-04","2020-10-07","2020-04-29","2019-01-19","2020-12-02","2019-02-11","2020-08-06","2019-07-18","2020-08-26","2020-10-10","2020-09-11","2020-10-07","2020-04-29","2020-12-02","2019-02-11","2019-07-18","2020-10-10","2020-04-29","2020-08-25","2019-11-04","2020-06-18","2019-11-04"};
        public static String[] LATEST_DATE = new String[] {"2020-06-09","2019-07-24","2019-09-10","2019-05-30","2019-03-04","2019-02-26","2019-06-18","2020-11-22","2019-07-03","2020-04-30","2020-10-09","2019-12-19","2020-04-22","2020-08-02","2020-04-18","2020-11-21","2020-06-13","2019-03-05","2021-01-16","2019-03-28","2020-09-20","2019-09-01","2020-10-10","2020-11-24","2020-10-26","2020-11-21","2020-06-13","2021-01-16","2019-03-28","2019-09-01","2020-11-24","2020-06-13","2020-10-09","2019-12-19","2020-08-02","2019-12-19",};

        public static void main(String[] args) throws IOException, InterruptedException {
            // 1. 获取HBase连接
            Configuration configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);

            // 2. 获取HTable
            String TABLE_NAME = "WATER_BILL";
            Table waterBillTable = connection.getTable(TableName.valueOf(TABLE_NAME));

            // 3. 构建数据
            String ROWKEY = "4944191";
            String CF_NAME = "C1";
            String COL_NAME = "NAME";
            String COL_ADDRESS = "ADDRESS";
            String COL_SEX = "SEX";
            String COL_PAY_DATE = "PAY_DATE";
            String COL_NUM_CURRENT = "NUM_CURRENT";
            String COL_NUM_PREVIOUS = "NUM_PREVIOUS";
            String COL_NUM_USAGE = "NUM_USAGE";
            String COL_TOTAL_MONEY = "TOTAL_MONEY";
            String COL_RECORD_DATE = "RECORD_DATE";
            String COL_LATEST_DATE = "LATEST_DATE";

            Integer MAX_LENGTH = 100000;
            Integer i = 0;

            while(i < MAX_LENGTH) {
                System.out.print((i + 1) + "/" + (MAX_LENGTH - 1) + ":");
                ROWKEY = RandomUtils.nextLong(10000000, 99999999) + "";
                System.out.println("构建ROWKEY:" + ROWKEY);
                // 4. 构建PUT
                Put put = new Put(Bytes.toBytes(ROWKEY));
                // 4.1 添加姓名列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_NAME)
                        , Bytes.toBytes(NAME_LIST[RandomUtils.nextInt(0, NAME_LIST.length)]));

                // 4.2 添加地址列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_ADDRESS)
                        , Bytes.toBytes(ADDR_LIST[RandomUtils.nextInt(0, ADDR_LIST.length)]));

                // 4.3 添加性别列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_SEX)
                        , Bytes.toBytes(SEX_LIST[RandomUtils.nextInt(0, SEX_LIST.length)]));

                // 4.4 添加缴费时间列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_PAY_DATE)
                        , Bytes.toBytes(PAY_DATE_LIST[RandomUtils.nextInt(0, PAY_DATE_LIST.length)]));

                // 4.5 添加表示数列（本次）
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_NUM_CURRENT)
                        , Bytes.toBytes(NUM_CURRENT_LIST[RandomUtils.nextInt(0, NUM_CURRENT_LIST.length)]));

                // 4.6 添加表示数列（上次）
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_NUM_PREVIOUS)
                        , Bytes.toBytes(NUM_CURRENT_LIST[RandomUtils.nextInt(0, NUM_PRE_LIST.length)]));

                // 4.7 添加用量列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_NUM_USAGE)
                        , Bytes.toBytes(USAGE_LIST[RandomUtils.nextInt(0, USAGE_LIST.length)]));

                // 4.8 添加合计金额列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_TOTAL_MONEY)
                        , Bytes.toBytes(TOTAL_MONEY[RandomUtils.nextInt(0, TOTAL_MONEY.length)]));

                // 4.9 添加查表日期列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_RECORD_DATE)
                        , Bytes.toBytes(RECORD_DATE[RandomUtils.nextInt(0, RECORD_DATE.length)]));

                // 4.10 添加最迟缴费日期列
                put.addColumn(Bytes.toBytes(CF_NAME)
                        , Bytes.toBytes(COL_LATEST_DATE)
                        , Bytes.toBytes(LATEST_DATE[RandomUtils.nextInt(0, LATEST_DATE.length)]));

                // 5. 执行put请求
                waterBillTable.put(put);

                ++i;
                System.out.println(i + " / " + MAX_LENGTH);
            }

            waterBillTable.close();
            connection.close();
        }
}
