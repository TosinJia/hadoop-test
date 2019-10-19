package com.tosin.hadoop.mapreduce.groupingcomparator;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@NoArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {
    public int compareTo(OrderBean o) {
        int ret;

        if(this.orderId.compareTo(o.getOrderId())>0){
            ret = 1;
        }else if(this.orderId.compareTo(o.getOrderId())<0){
            ret = -1;
        }else{
            ret = (this.price-o.getPrice())>0?-1:1;
        }
        return ret;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.productId);
        out.writeDouble(this.price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.price = in.readDouble();
    }
    // 订单编号
    private String orderId;
    // 产品编号
    private String productId;
    // 价格
    private double price;

    @Override
    public String toString() {
        return orderId + "\t" + productId + "\t" + price;
    }
}
