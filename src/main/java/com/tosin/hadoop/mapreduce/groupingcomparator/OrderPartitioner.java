package com.tosin.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {

        String orderId = orderBean.getOrderId();
        return (Integer.parseInt(orderId)&Integer.MAX_VALUE)%numPartitions;
    }
}
