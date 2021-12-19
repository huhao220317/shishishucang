package com.atguigu.gmall.mapper;

import com.atguigu.gmall.beans.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Author: Felix
 * Date: 2021/11/16
 * Desc: 地区统计Mapper接口
 */
public interface ProvinceStatsMapper {
    //获取某一天地区以及交易额
    @Select("select province_id,province_name,sum(order_amount) order_amount from province_stats_2021 " +
        " where toYYYYMMDD(stt) = #{date} group by province_id,province_name")
    List<ProvinceStats> selectProvinceStats(Integer date);
}
