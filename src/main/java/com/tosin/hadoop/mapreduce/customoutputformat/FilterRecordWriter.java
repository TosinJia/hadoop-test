package com.tosin.hadoop.mapreduce.customoutputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fsDataOutputStream;
    FSDataOutputStream otherFsDataOutputStream;
    public FilterRecordWriter(TaskAttemptContext job) {
        FileSystem fileSystem;
        try {
            // 1 获取文件系统
            fileSystem = FileSystem.get(job.getConfiguration());

            // 2 创建输出文件路径
            Path path = new Path("e:/itstar.log");
            Path otherPath = new Path("e:/other.log");
            // 3 创建输出流
            fsDataOutputStream = fileSystem.create(path);
            otherFsDataOutputStream = fileSystem.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("itstar")) {
            fsDataOutputStream.write(key.toString().getBytes());
        }else{
            otherFsDataOutputStream.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if(fsDataOutputStream != null){
            fsDataOutputStream.close();
        }
        if(otherFsDataOutputStream != null){
            otherFsDataOutputStream.close();
        }
    }
}
