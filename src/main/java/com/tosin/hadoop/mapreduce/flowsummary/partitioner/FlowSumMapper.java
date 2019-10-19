package com.tosin.hadoop.mapreduce.flowsummary.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();
        // 2 切分数据
        String[] fields = line.split("\t");
        // 3 封装对象
        // 取出手机号
        String phoneNumber = fields[1];
        // 取出上下行流量
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        // 4 写出
        context.write(new Text(phoneNumber), new FlowBean(upFlow, downFlow));
    }
}
