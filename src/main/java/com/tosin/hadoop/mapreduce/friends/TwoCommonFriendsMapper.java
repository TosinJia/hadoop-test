package com.tosin.hadoop.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class TwoCommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行数据 A	I,K,C,B,G,F,H,O,D,
        String line = value.toString();

        String[] friend2Perion = line.split("\t");
        String friend = friend2Perion[0];
        String[] persons = friend2Perion[1].split(",");

        // 排序
        Arrays.sort(persons);
        for (int i = 0; i < persons.length-1; i++) {
            for (int j = i+1; j < persons.length; j++) {
                // 2 写出 <人-人 好友>
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
            }
        }
    }
}
