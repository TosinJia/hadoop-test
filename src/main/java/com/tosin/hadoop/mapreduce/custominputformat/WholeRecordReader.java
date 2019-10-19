package com.tosin.hadoop.mapreduce.custominputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration conf;

    private BytesWritable value = new BytesWritable();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit)split;
        conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            // 1 定义缓存区
            byte[] contents = new byte[(int) fileSplit.getLength()];

            FileSystem fileSystem = null;
            FSDataInputStream fsDataInputStream = null;
            try{
                // 2 获取文件系统
                Path path = fileSplit.getPath();
                fileSystem = path.getFileSystem(conf);
                // 3 读取数据
                fsDataInputStream = fileSystem.open(path);
                // 4 读取文件内容
                IOUtils.readFully(fsDataInputStream, contents, 0, contents.length);
                // 5 输出文件内容
                value.set(contents, 0, contents.length);
            }catch (Exception e){

            }finally{
                IOUtils.closeStream(fsDataInputStream);
            }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}
