package com.tosin.hadoop.mapreduce.flowsummary.comparablePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求4：不同省份输出文件内部排序（部分排序）
 */
public class FlowSumSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumSortDriver.class);

        job.setMapperClass(FlowSumSortMapper.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FlowSumSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置自定义分区、设置reducetask个数
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);
        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
