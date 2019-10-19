package com.tosin.hadoop.mapreduce.flowsummary.comparablePartitioner;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        // 1 获取手机号前三位
        String phonePrefix = text.toString().substring(0, 3);

        // 2 根据手机号归属地设置分区
        int partition = 4;
        if("136".equals(phonePrefix)){
            partition = 0;
        }else if("137".equals(phonePrefix)){
            partition = 1;
        }else if("138".equals(phonePrefix)){
            partition = 2;
        }else if("139".equals(phonePrefix)){
            partition = 3;
        }
        return partition;
    }
}
