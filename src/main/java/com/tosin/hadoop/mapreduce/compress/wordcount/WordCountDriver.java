package com.tosin.hadoop.mapreduce.compress.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        // 开启reduce端输出压缩, 设置reduce端输出压缩方式
//        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
//        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 开启reduce端输出压缩
        FileOutputFormat.setCompressOutput(job, true);
        // 设置reduce端输出压缩方式
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
