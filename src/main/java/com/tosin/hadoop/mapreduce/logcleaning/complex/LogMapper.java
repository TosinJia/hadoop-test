package com.tosin.hadoop.mapreduce.logcleaning.complex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.rmi.runtime.Log;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行日志
        String line = value.toString();
        // 2 解析日志是否合法
        LogBean result = parseLog(line);
        if(!result.isValid()){
            context.getCounter("LOGLEGITIMATE", "FALSE").increment(1);
            return;
        }
        context.getCounter("LOGLEGITIMATE", "TRUE").increment(1);
        // 3 输出数据
        context.write(new Text(result.toString()), NullWritable.get());
    }

    /**
     * 解析日志
     * @param line
     * @return
     */
    private LogBean parseLog(String line) {
        // 1 切分数据
        String[] fields = line.split(" ");
        LogBean lb = new LogBean();
        if(fields.length > 11){
            // 2 封装数据
            lb.setRemoteAddr(fields[0]);
            lb.setRemoteUser(fields[1]);
            lb.setTimeLocal(fields[3]);
            lb.setRequest(fields[6]);
            lb.setStatus(fields[8]);
            lb.setBodyBytesSent(fields[9]);
            lb.setHttpReferer(fields[10]);

            if(fields.length > 12){
                lb.setHttpUserAgent(fields[11] + " " + fields[12]);
            }else{
                lb.setHttpUserAgent(fields[11]);
            }
            // 大于等于400，不合法日志
            if(Integer.parseInt(lb.getStatus()) >= 400){
                lb.setValid(false);
            }
        }else{
            lb.setValid(false);
        }
        return lb;
    }
}
