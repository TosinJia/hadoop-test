package com.tosin.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class HDFSClient {
    public static void main(String[] args) throws Exception {
//        testCopyFromLocalFile();
        practice191011();
    }

    public static void practice191011() throws IOException, URISyntaxException, InterruptedException {
        //生成文件
        String filePathName = "E:\\temp\\BIGDATA.txt";
        FileOutputStream fileOutputStream = new FileOutputStream(new File(filePathName));
        fileOutputStream.write("I LOVE BIGDATA!".getBytes());
        fileOutputStream.close();

        //上传文件到HDFS中
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
        fileSystem.copyFromLocalFile(true, new Path(filePathName), new Path("/BIGDATA.txt"));
        fileSystem.close();
        System.out.println("上传成功");
    }


    public static void testCopyFromLocalFile() throws IOException {
        //1. 创建配置信息对象
        Configuration conf = new Configuration();
        //2. 设置配置文件中的部分参数
        conf.set("fs.defaultFS", "hdfs://bd-01-01:9000");
        //3. 创建文件系统
        FileSystem fs = FileSystem.get(conf);
        //4. 把本地文件上传到文件系统中
        fs.copyFromLocalFile(new Path("E:\\log4j.properties"),
                new Path("/log4j-1.properties"));
        //5. 关闭资源
        fs.close();

        System.out.println("上传成功！");
    }
    public static void testCopyFromLocalFile1() throws Exception {
        //1. 创建配置信息对象
        Configuration conf = new Configuration();
        //2. 设置部分参数
        conf.set("dfs.replication", "2");
        //3. 使用HDFS地址创建文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
        //4. 本地文件路径
        Path src = new Path("E:\\log4j.properties");
        //5. 目标HDFS路径
        Path dst = new Path("hdfs://bd-01-01:9000/log4j.properties");
        //6. 以拷贝方式上传
        fs.copyFromLocalFile(src, dst);
        //7. 关闭
        fs.close();
    }
}
