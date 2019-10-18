package com.tosin.hadoop.mapreduce.logcleaning.complex;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 日志bean
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LogBean {
    private String remoteAddr;  //记录客户端的ip地址
    private String remoteUser;  //记录客户端用户名称,忽略属性"-"
    private String timeLocal;   //记录访问时间与时区
    private String request;     //记录请求的url与http协议
    private String status;      //记录请求状态；成功是200
    private String bodyBytesSent;   //记录发送给客户端文件主体内容大小
    private String httpReferer;     //用来记录从那个页面链接访问过来的
    private String httpUserAgent;   //记录客户浏览器的相关信息

    private boolean valid = true;   //判断数据是否合法

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(valid);
        sb.append("\001").append(remoteAddr);
        sb.append("\001").append(remoteUser);
        sb.append("\001").append(timeLocal);
        sb.append("\001").append(request);
        sb.append("\001").append(status);
        sb.append("\001").append(bodyBytesSent);
        sb.append("\001").append(httpReferer);
        sb.append("\001").append(httpUserAgent);
        return sb.toString();
    }
}
