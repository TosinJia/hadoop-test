package com.tosin.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean ob = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();
        // 2 切分数据
        String[] fields = line.split("\t");
        ob.setOrderId(fields[0]);
        ob.setProductId(fields[1]);
        ob.setPrice(Double.parseDouble(fields[2]));
        // 3 写出
        context.write(ob, NullWritable.get());
    }
}
