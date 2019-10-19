package com.tosin.hadoop.mapreduce.customoutputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 7.5 过滤日志及自定义日志输出路径案例（自定义OutputFormat）
 */
public class FilterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FilterDriver.class);
        job.setMapperClass(FilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定义输出格式类
        job.setOutputFormatClass(FilterOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 虽然我们自定义了ouputformat，但是因为我们的outputformat继承自FileOutputFormat,
        // 而FileOutputFormat要输出一个 ._SUCCESS.crc 文件，所以还需要指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
