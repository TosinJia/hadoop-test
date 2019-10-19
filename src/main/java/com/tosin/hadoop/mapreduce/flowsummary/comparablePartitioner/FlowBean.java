package com.tosin.hadoop.mapreduce.flowsummary.comparablePartitioner;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
public class FlowBean implements WritableComparable<FlowBean> {
    public int compareTo(FlowBean o) {
        //倒序排列?
        return this.sumFlow > o.getSumFlow()?-1:1;
    }
    public void set(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }
    // 3 序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }
    // 4 反序列化
    // 5 反序列化读取顺序要和序列化写入顺序一致
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    // 6 重写toString方法，方便打印到文本
    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }
}
