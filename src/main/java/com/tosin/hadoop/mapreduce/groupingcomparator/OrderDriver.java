package com.tosin.hadoop.mapreduce.groupingcomparator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 7.3 辅助排序和二次排序案例（GroupingComparator）
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置reduce端分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        // 设置自定义分区
        job.setPartitionerClass(OrderPartitioner.class);
        // 设置reduce个数(part-r-)
        job.setNumReduceTasks(3);
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
