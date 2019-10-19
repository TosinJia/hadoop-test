package com.tosin.hadoop.mapreduce.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) a;
        int ret = 0;

        if(aBean.getOrderId().compareTo(bBean.getOrderId())>0){
            ret = 1;
        }else if(aBean.getOrderId().compareTo(bBean.getOrderId())<0){
            ret = -1;
        }
        return ret;
    }
}
