package com.tosin.hadoop.mapreduce.wordcount.asciiPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException {

        //中文编码
        String line = new String(value.getBytes(), 0, value.getLength(), "UTF-8");
        // 1 获取一行数据
        line = value.toString();
        // 2 切割
        String[] words = line.split(" ");
        // 3 输出
        for (String word:words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
