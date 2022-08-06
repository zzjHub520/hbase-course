# HBase第三天课堂笔记

## HBase的读流程

* 客户端拿到一个rowkey（首先得要知道这个rowkey存在哪个region中）
* 根据zk获取hbase:meta表，这个表中存放了region的信息，根据namespace、表名，就可以根据rowkey查看是否匹配某个region的startkey、endkey，返回region的信息
* 还需要查询region是在哪个HRegionServer（因为我们是不知道region会在存在什么地方的）
* 读取Store
	* 优先读取写缓存（MemStore）
	* 读取BlockCache（LRUBlockCache、BucketBlockCache）
	* 再读取HFile

## HBase的写数据

写数据的流程：

（1-3是和读流程是类似的，都需要找到当前要写入的rowkey，应该存放在哪个region、哪个region server）

1. 客户端拿到一个rowkey（首先得要知道这个rowkey存在哪个region中）
2. 根据zk获取hbase:meta表，这个表中存放了region的信息，根据namespace、表名，就可以根据rowkey查看是否匹配某个region的startkey、endkey，返回region的信息
3. 还需要查询region是在哪个HRegionServer（因为我们是不知道region会在存在什么地方的）
4. 首先要将数据写入到MemStore中
5. MemStore大小到达128M、MemStore数据已经超出一小时，会自动Flush到HDFS中的HFile
6. compaction合并
	1. 一阶段合并：如果每一个MemStore写满后，都会一溢写到HFile中，这样会有很多的HFile，对将来的读取不利。所以需要将这些小的HFile合并成大一点的HFile
	2. 二阶段合并：将所有的HFile合并成一个HFile

HBase 2.0+ In memory compaction（总共的流程为三个阶段的合并）

* In memory comapaction主要是延迟flush到磁盘的时间，尽量优先写入到内存中，有一系列的合并优化操作
* 数据都是以segment（段）来保存的，首先数据会写到active segment，active segment写完后会将segment合并到piepline里面，合并pipeline的之后会有一定的策略
	* basic：只管存，合并，不会优化重复数据
	* eager：会将一些重复数据进行优化
	* adaptive：会根据重复度来进行优化合并
* pipeline如果到达一定的阈值，就开始Flush

### 写数据的两阶段合并

* minor compaction
	* 比较轻量级的，耗时比较短。一般一次不用合并太多（推荐：3个文件）
	* 每一个memstore写满后，会flush，形成storefiles
	* 如果storefiles多了之后，对读取是不利
	* 所以storefiles需要合并
* major compaction
	* 比较重量级的操作，在HBase读写并发比较高的时候，尽量要避免这类操作。默认是7天一检查，进行major compaction
	* 将所有的storefiles合并成1个最终的storefile

```xml
<property>
<name>hbase.hregion.majorcompaction</name>
<value>604800000</value>
<source>hbase-default.xml</source>
</property>
```

## Region的管理

* HMaster负责Region的管理
* Region的分配
	* HMaster会负责Region的分配，因为当前的集群中有很多的HRegionServer，HMaster得明确不同HRegionServer的负载，然后将Region分配给对应的HRegionServer
* RegionServer的上线
	* RegionServer是通过往ZK中写节点，HMaster可以监听节点，发现新上线的RegionServer
	* 后续HMaster可以将region分配给新上线的RegionServer
* RegionServer的下线
	* RegionServer下线也是通过ZK，HMaster可以监控到某个HRegionServer对应ZK节点的变化，如果节点不存在，认为该RegionServer已经挂了
	* 将RegionServer移除
* Region的分裂
	* Region的大小达到一定的阈值，HMaster会控制Region进行分裂
	* 按照startkey、endkey取一个midkey，来分裂成两个region，原有的region下线
	* 自动分区（分裂的过程，一个region分配到不同的HRegionServer中，保证用多台服务器来处理并发请求）
		* 如果数据量大，推荐使用手动分区

### Master上线和下线

* 一个集群中会存在多个Master的情况，但是只有一个ActiveMaster，其他的Backup Master，监听ZK的节点，如果Active Master crash了，其他的backup master就会进行切换
* Master持有RegionServers，哪些RegionServers是有效的，RegionServer下线Master是可以获取的
* Master如果crash，会导致一些管理性质的工作无法执行，创建表、删除表...会操作失败，但数据型操作是可以继续的

### 编写Bulkload的MR程序

### 理解bulkload

* Bulkload是将数据导入的时候可以批量将数据直接生成HFile，放在HDFS中，避免直接和HBase连接，使用put进行操作
* 绕开之前将的写流程
	* WAL
	* MemStore
	* StoreFile合并
* 批量写的时候效果高

### 如何mapper

* 如果实现一个MR的Mapper
	* 实现一个Mapper必须要指定4个数据类型
	* 从Mapper继承
		1. KeyIn：LongWritable
		2. ValueIn：Text
		3. KeyOut：自己指定（必须是Hadoop的类型——Hadoop有自己的一套序列化类型）
		4. ValueOut：自己指定

### 实现Mapper

* ImmutableBytesWritable——表示在Hbase对MapReduce扩展实现的类型， 对应rowkey
* MapReduceExtendedCell——表示单元格，也是hbase支持MapReduce实现的类型
	* 可以使用HBase的KeyValue来构建

```java
public class BankRecordMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, MapReduceExtendedCell> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将Mapper获取到Text文本行，转换为TransferRecord实体类
        // 7e59c946-b1c6-4b04-a60a-f69c7a9ef0d6,SU8sXYiQgJi8,6225681772493291,杭州银行,丁杰,4896117668090896,
        // 卑文彬,老婆，节日快乐,电脑客户端,电子银行转账,转账完成,2020-5-13 21:06:92,11659.0
        TransferRecord transferRecord = TransferRecord.parse(value.toString());

        // 从实体类中获取ID，并转换为rowkey
        String rowkeyString = transferRecord.getId();
        byte[] rowkeyByteArray = Bytes.toBytes(rowkeyString);
        byte[] columnFamily = Bytes.toBytes("C1");
        byte[] colId = Bytes.toBytes("id");
        byte[] colCode = Bytes.toBytes("code");
        byte[] colRec_account = Bytes.toBytes("rec_account");
        byte[] colRec_bank_name = Bytes.toBytes("rec_bank_name");
        byte[] colRec_name = Bytes.toBytes("rec_name");
        byte[] colPay_account = Bytes.toBytes("pay_account");
        byte[] colPay_name = Bytes.toBytes("pay_name");
        byte[] colPay_comments = Bytes.toBytes("pay_comments");
        byte[] colPay_channel = Bytes.toBytes("pay_channel");
        byte[] colPay_way = Bytes.toBytes("pay_way");
        byte[] colStatus = Bytes.toBytes("status");
        byte[] colTimestamp = Bytes.toBytes("timestamp");
        byte[] colMoney = Bytes.toBytes("money");

        // 构建输出key：new ImmutableBytesWrite(rowkey)
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowkeyByteArray);

        // 使用KeyValue类构建单元格，每个需要写入到表中的字段都需要构建出来单元格
        KeyValue kvId = new KeyValue(rowkeyByteArray, columnFamily, colId, Bytes.toBytes(transferRecord.getId()));
        KeyValue kvCode = new KeyValue(rowkeyByteArray, columnFamily, colCode, Bytes.toBytes(transferRecord.getCode()));
        KeyValue kvRec_account = new KeyValue(rowkeyByteArray, columnFamily, colRec_account, Bytes.toBytes(transferRecord.getRec_account()));
        KeyValue kvRec_bank_name = new KeyValue(rowkeyByteArray, columnFamily, colRec_bank_name, Bytes.toBytes(transferRecord.getRec_bank_name()));
        KeyValue kvRec_name = new KeyValue(rowkeyByteArray, columnFamily, colRec_name, Bytes.toBytes(transferRecord.getRec_name()));
        KeyValue kvPay_account = new KeyValue(rowkeyByteArray, columnFamily, colPay_account, Bytes.toBytes(transferRecord.getPay_account()));
        KeyValue kvPay_name = new KeyValue(rowkeyByteArray, columnFamily, colPay_name, Bytes.toBytes(transferRecord.getPay_name()));
        KeyValue kvPay_comments = new KeyValue(rowkeyByteArray, columnFamily, colPay_comments, Bytes.toBytes(transferRecord.getPay_comments()));
        KeyValue kvPay_channel = new KeyValue(rowkeyByteArray, columnFamily, colPay_channel, Bytes.toBytes(transferRecord.getPay_channel()));
        KeyValue kvPay_way = new KeyValue(rowkeyByteArray, columnFamily, colPay_way, Bytes.toBytes(transferRecord.getPay_way()));
        KeyValue kvStatus = new KeyValue(rowkeyByteArray, columnFamily, colStatus, Bytes.toBytes(transferRecord.getStatus()));
        KeyValue kvTimestamp = new KeyValue(rowkeyByteArray, columnFamily, colTimestamp, Bytes.toBytes(transferRecord.getTimestamp()));
        KeyValue kvMoney = new KeyValue(rowkeyByteArray, columnFamily, colMoney, Bytes.toBytes(transferRecord.getMoney()));

        // 使用context.write将输出输出
        // 构建输出的value：new MapReduceExtendedCell(keyvalue对象)
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvId));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvCode));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvRec_account));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvRec_bank_name));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvRec_name));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvPay_account));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvPay_name));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvPay_comments));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvPay_channel));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvPay_way));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvStatus));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvTimestamp));
        context.write(immutableBytesWritable, new MapReduceExtendedCell(kvMoney));
    }
}
```



异常：报错，连接2181失败，仔细看是连接的本地的localhost的zk，本地是没有ZK

解决办法：

* Job.getInstance(configuration)
* 需要把HBaseConfiguration加载的配置文件传到JOB中

```java
 INFO - Opening socket connection to server 127.0.0.1/127.0.0.1:2181. Will not attempt to authenticate using SASL (unknown error)
 WARN - Session 0x0 for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused: no further information
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717)
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:361)
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1141)
 WARN - 0x59321afb to localhost:2181 failed for get of /hbase/hbaseid, code = CONNECTIONLOSS, retries = 1
 INFO - Opening socket connection to server 0:0:0:0:0:0:0:1/0:0:0:0:0:0:0:1:2181. Will not attempt to authenticate using SASL (unknown error)
 WARN - Session 0x0 for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused: no further information
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717)
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:361)
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1141)
```

### MapReduce实现过程

* Mapper<keyin, keyout, valuein, valueout>
* Driver

```java
public class BankRecordBulkLoadDriver {
    public static void main(String[] args) throws Exception {
        // 1.	使用HBaseConfiguration.create()加载配置文件
        Configuration configuration = HBaseConfiguration.create();
        // 2.	创建HBase连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 3.	获取HTable
        Table table = connection.getTable(TableName.valueOf("ITCAST_BANK:TRANSFER_RECORD"));

        // 4.	构建MapReduce JOB
        // a)	使用Job.getInstance构建一个Job对象
        Job job = Job.getInstance(configuration);
        // b)	调用setJarByClass设置要执行JAR包的class
        job.setJarByClass(BankRecordBulkLoadDriver.class);
        // c)	调用setInputFormatClass为TextInputFormat.class
        job.setInputFormatClass(TextInputFormat.class);
        // d)	设置MapperClass
        job.setMapperClass(BankRecordMapper.class);
        // e)	设置输出键Output Key Class
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        // f)	设置输出值Output Value Class
        job.setOutputValueClass(MapReduceExtendedCell.class);

        // g)	设置输入输出到HDFS的路径，输入路径/bank/input，输出路径/bank/output
        // i.	FileInputFormat.setInputPaths
        FileInputFormat.setInputPaths(job, new Path("hdfs://node1.itcast.cn:8020/bank/input"));
        // ii.	FileOutputFormat.setOutputPath
        FileOutputFormat.setOutputPath(job, new Path("hdfs://node1.itcast.cn:8020/bank/output"));

        // h)	使用connection.getRegionLocator获取HBase Region的分布情况
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf("ITCAST_BANK:TRANSFER_RECORD"));
        // i)	使用HFileOutputFormat2.configureIncrementalLoad配置HFile输出
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

        // 5.	调用job.waitForCompletion执行MapReduce程序
        if(job.waitForCompletion(true)) {
            System.exit(0);
        }
        else {
            System.exit(1);
        }
    }
}

```

## 协处理器

### 理解下RDMS的触发器、存储过程

* 触发器：当执行一些insert/delete/update之类的操作，在操作之前、之后可以执行一些逻辑（这些逻辑就是易于SQL的脚本——PL/SQL，来实现）
* 存储过程：直接在数据库服务器端编写一段逻辑，这个逻辑可以被客户端来调用

### HBase的协处理器

* observer：拦截put/get/scan/delete之类的操作，执行协处理器对应的代码（Java实现的——将Java实现好的协处理器直接打成一个JAR包，JAR包可以放在HDFS上，部署到HBase）。例如：Phoenix插入一条数据，同时更新索引
* endpoint：可以使用Java编写一些逻辑，将JAR包部署到HBase，就可以实现一些扩展的功能。例如：Phoenix的select count\max.....

## 常见数据结构理解

* 跳表
	* 链表是单层的，跳表是多层，层数越小，越稀疏
	* 可以理解为给有序链表增加稀疏索引，加快查询效率
* 二叉搜索树
	* 一个节点最多有两个节点
	* 二叉搜索树是一种排序树
	* 一般数据库中索引不会用二叉搜索树，因为有两个问题
		* 二叉树的高度问题（越高、查询效率下降、IO操作越多）
		* 二叉树的平衡问题（如果不平衡，就会导致搜索某些节点的时候，效率很多，有的直属高度很高，有的很低——负载不均衡，如果及其不平衡，退化成链表——老歪脖子树）
* 平衡二叉树
	* 要求很严格
	* 要求：任意节点的子树的高度差不能超过1
	* 要求过于严格，对插入、删除有较大影响，因为每次插入、删除要进行节点的一些旋转一些操作，都要确保树是严格的平衡的
* 红黑树
	* 弱平衡要求的二叉树
	* 有红色、黑色两种节点，叶子节点都是NIL节点（无特殊意义的节点）
	* 每个红色节点都有两个黑色节点
	* 要求：任意的红色节点到叶子节点有相同数量的黑色节点
	* 在一些Java TreeMap是基于红黑树实现的
* B树
	* 多路搜索树，也是平衡的，又多叉的情况存在
	* 在基于B树搜索的时候，在每一层分布都有数据节点，只要找到我们想要的数据，就可以直接返回
* B+树
	* B+树是B树的升级版本
	* 所有的数据节点都在最后一层（叶子节点），而且叶子节点之间彼此是连接的
	* 应用场景：MySQL B+tree索引、文件系统

## LSM树

* LSM树：这种树结构是多种结构的组合
* 为了保证写入的效率，对整个结构进行了分层，C0、C1、C2....
* 写入数据的时候，都是写入到C0，就要求C0的写入是很快的，例如：HBase写的就是MemStore——跳表结构（也有其他用红黑树之类的）
* C0达到一定的阈值，就开始刷写到C1，进行合并，Compaction
* C1达到一定的条件，也就即席合并到C2
* 存在磁盘中的C1\C2层的数据一般是以B+树方式存储，方便检索

* WAL预写日志：首先写数据为了避免数据丢失，一定要写日志,WAL会记录所有的put/delete操作之类的，如果出现问题，可以通过回放WAL预写日志来恢复数据
	* WAL预写日志：是写入HDFS中，是以SequenceFile来存储的，而且是顺序存储的（为了保证效率），PUT/DELETE操作都是保存一条数据
* 比较适合写多读少的场景，如果读取比较多，需要创建二级索引

## 布隆过滤器（BloomFilter）

布隆过滤器判断的结果：

1. 不存在
2. 可能存在



* 布隆过滤器是一种结构，也是一种BitMap结构，因为Bitmap占用的空间小，所以布隆过滤器经常使用在一些海量数据判断元素是否存在的场景，例如：HBase
* 写入key/value键值对的时候，会对这个key进行k个哈希函数取余（取Bitmap的长度），得到k个数值，这个k个数值一定是在这个Bitmap中的，值要么是0、要么是1
* 根据key来进行判断的时候，首先要对这个key进行k个哈希函数取余，判断取余之后的k个值，在Bitmap时候都已经被设置为1，如果说有一个不是1，表示这个key一定是不存在的。如果是全都是1，可能存在。

## HBase中StoreFile的结构

* StoreFile是分了不同的层，每一次层存储的数据是不一样
* 主要记住：
	* Data Block——保存实际的数据
	* Data Block Index——数据块的索引，查数据的时候先查Index，再去查数据
	* DataBlock里面的数据也是有一定的结构
		* Key的长度
		* Value的长度
		* Key的数据结构比较丰富：rowkey、family、columnname、keytype（put、delete）
		* Value就是使用byte[]存储下来即可

## HBase优化

每个集群会有系统配置，社区一定会把一些通用的、适应性强的作为默认配置，有很多都是折中的配置。很多时候，出现问题的时候，我们要考虑优化。



* 通用优化
	* 跟硬件有一定的关系，SSD、RAID（给NameNode使用RAID1架构，可以有一定容错能力）
* 操作系统优化
	* 最大的开启文件数量（集群规模大之后，写入的速度很快，经常要Flush，会在操作系统上同时打开很多的文件读取）
	* 最大允许开启的进程
* HDFS优化
	* 副本数
	* RPC的最大数量
	* 开启的线程数
* HBase优化
	* 配置StoreFile大小
	* 预分区
	* 数据压缩
	* 设计ROWKEY
	* 开启BLOOMFILER
	* 2.X开启In-memory Compaction
	* ...
* JVM
	* 调整堆内存大小
	* 调整GC，并行GC，缩短GC的时间