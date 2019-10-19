package com.tosin.hadoop.mapreduce.flowsummary.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNumber = text.toString();
        // 1 获取电话号码前三位
        String phonePrefix = phoneNumber.substring(0, 3);

        //注：如果设置的分区数小于下面的分区数，如3、则最后一个分区混数据分区
        //注：如何设置的分区数大于下面的分区数，如5，则报错
        int partition = 4;
        // 2 根据前三位判断省份，指定不同分区
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
