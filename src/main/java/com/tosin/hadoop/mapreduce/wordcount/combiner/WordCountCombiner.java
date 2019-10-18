package com.tosin.hadoop.mapreduce.wordcount.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 局部汇总
 * 代码和WordCountReducer完全相同
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1 汇总
        int sum = 0;
        for (IntWritable i: values) {
            sum += i.get();
        }
        //2 写出
        context.write(key, new IntWritable(sum));
    }
}
