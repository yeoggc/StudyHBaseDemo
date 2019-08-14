package com.ggc.study_hbase.hbase_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class HDFS2HBaseDemo {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //得到Configuration
        Configuration conf = HBaseConfiguration.create();

        //创建Job任务
        Job job = Job.getInstance(conf, HDFS2HBaseDemo.class.getSimpleName());
        job.setJarByClass(HDFS2HBaseDemo.class);
        Path inPath = new Path("hdfs://ggc:9000/input/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

        //设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitToHBaseReducer.class, job);

        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }

    }


    static class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            byte[] rowKeyBytes = Bytes.toBytes(split[0]);

            Put put = new Put(rowKeyBytes);
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[1]));

            context.write(new ImmutableBytesWritable(rowKeyBytes), put);

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

}
