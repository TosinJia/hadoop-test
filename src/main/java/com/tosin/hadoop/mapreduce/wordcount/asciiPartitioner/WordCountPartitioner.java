package com.tosin.hadoop.mapreduce.wordcount.asciiPartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        //1 获取单词首字符ascii
        String word = text.toString();
        byte ascii = (byte)word.charAt(0);
        //2 根据ascii奇偶数分区
        if(ascii % 2 ==0){
            return 0;
        }else{
            return 1;
        }
    }

}
