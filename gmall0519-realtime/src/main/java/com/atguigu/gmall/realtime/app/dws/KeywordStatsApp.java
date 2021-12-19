package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeyWordUDTF;
import com.atguigu.gmall.realtime.beans.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1,基本环境的准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 参数1 函数名
        tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);
        // TODO 2,检查点相关的设置
        // TODO 3,从Kafka中读取数据，创建动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";

        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
                "WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");
        //关键词只有搜索行为才有，而只有page_id是good_list时才是搜索行为 item标签是搜索词
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view  " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");

        //TODO 5.利用udtf将数据拆分
        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");
        //TODO 6.根据各个关键词出现次数进行ct
        Table keywordStatsSearch  = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   "+keywordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);
        keywordStatsDS.addSink(ClickHouseUtil.<KeywordStats>getJdbcSink(
                "insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"
        ));
        env.execute();
    }
}
