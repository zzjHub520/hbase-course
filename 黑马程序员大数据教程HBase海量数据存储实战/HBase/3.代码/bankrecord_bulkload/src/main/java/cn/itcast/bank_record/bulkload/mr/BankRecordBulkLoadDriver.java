package cn.itcast.bank_record.bulkload.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

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
