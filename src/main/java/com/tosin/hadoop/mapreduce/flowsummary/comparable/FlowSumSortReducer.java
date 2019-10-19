package com.tosin.hadoop.mapreduce.flowsummary.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //循环输出避免总流量相同的情况
        for(Text phoneNumber: values){
            context.write(phoneNumber, key);
        }
    }
}
