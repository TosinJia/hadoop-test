package com.tosin.hadoop.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OneCommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据 A:B,C,D,F,E,O A的好友有B,C,D,F,E,O
        String line = value.toString();
        // 2 切分数据
        String[] fields = line.split(":");
        // 3 获取 人 和 好友
        String person = fields[0];
        String[] friends = fields[1].split(",");
        // 4 输出 B A,C A,D A,F A,E A,O A 好友是A的好友
        for(String friend: friends){
            // 好友 人
            context.write(new Text(friend), new Text(person));
        }
    }
}
