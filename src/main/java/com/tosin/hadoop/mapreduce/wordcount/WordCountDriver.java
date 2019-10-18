package com.tosin.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求1：统计一堆文件中单词出现的次数
 * */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        args = new String[]{"E:\\temp\\wordcount\\in\\", "E:\\temp\\wordcount\\out-01"};

        // 1 创建配置，获取Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置jar包（驱动类）
        job.setJarByClass(WordCountDriver.class);
        // 3 设置自定义Mapper、Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置Map阶段输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置Reduce阶段输出数据类型（最终数据类型）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6 设置原数据文件存放路径，结果数据输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //  Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:/E:/temp/wordcount/out already exists
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7 job提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
