package com.tosin.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @auther: TosinJia
 * @date: 2019/10/11 14:52
 * @description:
 */
public class TestHDFSClient {
    private static FileSystem fileSystem;
    @Before
    public void initFileSystem() throws Exception {
        //1. 创建配置信息对象
        Configuration conf = new Configuration();
        //2. 设置部分参数
        conf.set("dfs.replication", "2");
        //3. 使用HDFS地址创建文件系统
        //final URI uri     ：HDFS地址
        //final Configuration conf：配置信息
        // String user ：Linux用户名
        fileSystem = FileSystem.get(new URI("hdfs://bd-01-01:9000"), conf, "root");
    }
    @After
    public void closeResources() throws IOException {
        //7. 关闭
        fileSystem.close();
        System.out.println("over");
    }

    /**
     * 判断文件文件夹 ls
     * @throws IOException
     */
    @Test
    public void listStatus() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for(FileStatus fileStatus: fileStatuses){
            String fileName = fileStatus.getPath().getName();
            if(fileStatus.isFile()){
                System.out.println("文件：" + fileName);
            }else{
                System.out.println("目录：" + fileName);
            }
        }
    }
    /**
     * 查看文件详情 ls
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()){
            LocatedFileStatus fileStatus = remoteIterator.next();
            String fileName = fileStatus.getPath().getName();
            long blockSize = fileStatus.getBlockSize();
            FsPermission permission = fileStatus.getPermission();
            long len = fileStatus.getLen();
            StringBuffer info = new StringBuffer();
            info.append("文件名：" + fileName).append("\r\n");
            info.append("\t"+(fileStatus.isFile()?"文件":"目录")).append("\r\n");
            info.append("\t块大小：" + blockSize).append("\r\n");
            info.append("\t权限：" + permission).append("\r\n");
            info.append("\t长度：" + len).append("\r\n");

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation: blockLocations) {
                long offset = blockLocation.getOffset();
                info.append("\tblock-offset:" + offset).append("\r\n");
                String[] hosts = blockLocation.getHosts();
                for (String host: hosts) {
                    info.append("\t\thost:" + host).append("\r\n");
                }
            }
            System.out.println(info.toString());
        }
    }
    /**
     * 重命名文件、文件夹 mv
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        fileSystem.rename(new Path("hdfs://bd-01-01:9000/log4j.properties"), new Path("hdfs://bd-01-01:9000/log4j-newname.properties"));
    }
    /**
     * 删除文件、文件夹 rm
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        //Path var1   : HDFS地址
        //boolean var2 : 是否递归删除
        fileSystem.delete(new Path("hdfs://bd-01-01:9000/tosin"), true);
    }
    /**
     * 创建目录 mkdir
     * @throws IOException
     */
    @Test
    public void mkdirs() throws IOException {
        fileSystem.mkdirs(new Path("hdfs://bd-01-01:9000/tosin/study"));
    }

    /**
     * HDFS文件下载
     * get
     */
    @Test
    public void copyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //boolean delSrc:是否将原文件删除
        //Path src ：要下载的路径
        //Path dst ：要下载到哪
        //boolean useRawLocalFileSystem ：是否校验文件
        fileSystem.copyToLocalFile(false, new Path(""), new Path(""), true);
    }


    /**
     * copyFromLocal
     * HDFS文件上传
     * 注：上传内容大于128MB，则是多个块
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile1() throws Exception {
        //4. 本地文件路径
        Path src = new Path("E:\\log4j.properties");
        //5. 目标HDFS路径
        Path dst = new Path("hdfs://bd-01-01:9000/log4j.properties");
        //6. 以拷贝方式上传
        fileSystem.copyFromLocalFile(src, dst);
    }
}
