package com.tosin.hadoop.mapreduce.flowsummary.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        //1 遍历所有FlowBean，将上下行流量分别累加
        for (FlowBean fb:values) {
            sumUpFlow += fb.getUpFlow();
            sumDownFlow += fb.getDownFlow();
        }
        //2 封装对象
        FlowBean value = new FlowBean(sumUpFlow, sumDownFlow);
        //3 写出
        context.write(key, value);
    }
}
