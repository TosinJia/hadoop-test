package com.tosin.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @auther: TosinJia
 * @date: 2019/10/11 17:03
 * @description: 通过IO流操作HDFS
 */
public class TestHDFSIO {
    /**
     * HDFS文件上传
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException, InterruptedException {
        //1. 创建配置文件信息
        Configuration conf = new Configuration();
        //2. 获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
        //3. 创建输入流
        FileInputStream fileInputStream = new FileInputStream(new File("E:\\temp\\hadoop-2.8.5.tar.gz"));
        //4. 创建输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/tosin/hadoop-2.8.5.tar.gz"));
        //5. 流对接
        try {
            //InputStream in    输入
            //OutputStream out  输出
            //int buffSize      缓冲区
            //boolean close     是否关闭流
            IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 4*1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fsDataOutputStream);
            IOUtils.closeStream(fileInputStream);
            System.out.println("upload success");
        }
    }

    /**
     * HDFS文件下载
     */
    @Test
    public void downloadFile() throws URISyntaxException, InterruptedException, IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
        // 打开输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/log4j-191011.properties"));
        //输出到控制台
        IOUtils.copyBytes(fsDataInputStream, System.out, 4*1024, true);
    }

    /**
     * 分块下载 定位文件读取
     * 1. IO读取第一块数据
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFlieSeek1() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
        //打开输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/tosin/hadoop-2.8.5.tar.gz"));
        //创建文件输出流
        FileOutputStream fileOutputStream = new FileOutputStream("E:\\temp\\hadoop-2.8.5-synthesis-1");

        //流对接
        byte[] buf = new byte[1024];
        for(int i=0; i < 128*1024;i++){
            fsDataInputStream.read(buf);
            fileOutputStream.write(buf);
        }
        //关闭流
        IOUtils.closeStream(fsDataInputStream);
        IOUtils.closeStream(fileOutputStream);
    }

    /**
     * 2. IO读取第二块数据
     * 3. 合并文件
     * cmd
     *      E:\temp>type hadoop-2.8.5-synthesis-2 >> hadoop-2.8.5-synthesis-1
     * 修改后缀
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFlieSeek2() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/tosin/hadoop-2.8.5.tar.gz"));
        FileOutputStream fileOutputStream = new FileOutputStream("E:\\temp\\hadoop-2.8.5-synthesis-2");

        // 定位偏移量/offset/游标/读取进度 (目的：找到第一块的尾巴，第二块的开头)
        fsDataInputStream.seek(128*1024*1024);
        // 流对接
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, 1024);

        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fsDataInputStream);
    }
}
