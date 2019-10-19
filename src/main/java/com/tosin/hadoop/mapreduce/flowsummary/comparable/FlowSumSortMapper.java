package com.tosin.hadoop.mapreduce.flowsummary.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean k = new FlowBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();
        // 2 切分数据
        String[] fields = line.split("\t");
        // 3 封装对象
        // 取出手机号
        String phoneNumber = fields[0];
        // 取出上下行流量
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        // 4 写出
        k.set(upFlow, downFlow);
        v.set(phoneNumber);
        context.write(k, v);
    }
}
