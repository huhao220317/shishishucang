package com.atguigu.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class DateTimeUtil {
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //将日期对象 转换为日期字符串
    public static String toYmdHms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        String dateStr = dtf.format(localDateTime);
        return dateStr;
    }

    //将日志字符串转换为时间毫秒数
    public static Long toTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        Long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }

}
