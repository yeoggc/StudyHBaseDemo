package com.ggc.study_hbase.hbase_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * 需求：
 * 实现读取HBase的一个张表中某一个列，保存到HBase的一个新表中
 */
public class HBase2HBaseDemo {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = HBaseConfiguration.create();

        Job job = Job.getInstance(conf, HBase2HBaseDemo.class.getSimpleName());

        Scan scan = new Scan();
        scan.setMaxVersions();
        TableMapReduceUtil.initTableMapperJob("fruit", scan
                , ReadFruitFromHBaseMapper.class, ImmutableBytesWritable.class, Put.class, job);


        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitToHBaseReducer.class
                , job, HRegionPartitioner.class);


        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }
    }

    static class WriteFruitToHBaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

            for (Put value : values) {
                context.write(NullWritable.get(), value);
            }

        }
    }

    static class ReadFruitFromHBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {


            Put put = new Put(key.get());
            //遍历添加column行
            for (Cell cell : value.rawCells()) {
                //添加 /克隆列族:info
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        put.add(cell);
                    }
                }

            }

            context.write(key, put);

        }
    }

}


