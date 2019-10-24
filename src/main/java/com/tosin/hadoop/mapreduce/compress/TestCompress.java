package com.tosin.hadoop.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        compress("E:\\temp\\log4j.properties", "org.apache.hadoop.io.compress.DefaultCodec");
//        compress("E:\\temp\\log4j.properties", "org.apache.hadoop.io.compress.GzipCodec");
//        compress("E:\\temp\\log4j.properties", "org.apache.hadoop.io.compress.BZip2Codec");
        //this version of libhadoop was built without snappy support.
//        compress("E:\\temp\\log4j.properties", "org.apache.hadoop.io.compress.SnappyCodec");

//        decompress("E:\\temp\\log4j.properties.gz", ".txt");
//        decompress("E:\\temp\\log4j.properties.bz2", ".txt");
    }

    /**
     * 压缩
     */
    public static void compress(String fileName, String codec) throws IOException, ClassNotFoundException {
        // 创建文件输入流
        FileInputStream fileInputStream = new FileInputStream(new File(fileName));
        // 通过反射获取编解码类
        Class<?> codecClass = Class.forName(codec);
        // 通过反射工具类实例化压缩编解码器对象，需要conf对象
        CompressionCodec compressionCodec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, new Configuration());
        // 创建文件输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName + compressionCodec.getDefaultExtension()));
        // 压缩编解码器创建压缩输出流
        CompressionOutputStream compressionOutputStream = compressionCodec.createOutputStream(fileOutputStream);
        // 流拷贝
        IOUtils.copyBytes(fileInputStream, compressionOutputStream, 5*1024*1024, false);
        // 关闭流
        compressionOutputStream.close();
        fileOutputStream.close();
        fileInputStream.close();
    }

    /**
     * 解压
     */
    public static void decompress(String fileName, String fileSuffix) throws IOException {
        // 创建压缩编解码器工厂
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(new Configuration());
        // 获取压缩编解码器
        CompressionCodec compressionCodec = compressionCodecFactory.getCodec(new Path(fileName));
        if(compressionCodec == null){
            System.out.println(fileName+"\t未能获取压缩编解码器");
            return;
        }
        // 使用压缩编解码器创建压缩输入流
        CompressionInputStream compressionInputStream = compressionCodec.createInputStream(new FileInputStream(new File(fileName)));
        // 创建文件输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName + fileSuffix));
        // 流拷贝
        IOUtils.copyBytes(compressionInputStream, fileOutputStream, 5*1024*1024, true);
    }
}
