package com.tosin.hadoop.mapreduce.friends;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneCommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 1 拼接 人
        StringBuffer sb = new StringBuffer();
        for(Text person: values){
            sb.append(person).append(",");
        }
        // 2 写出 好友 有好友的所有人
        context.write(key, new Text(sb.toString()));
    }
}
