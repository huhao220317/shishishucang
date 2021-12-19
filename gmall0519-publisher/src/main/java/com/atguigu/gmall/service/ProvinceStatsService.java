package com.atguigu.gmall.service;

import com.atguigu.gmall.beans.ProvinceStats;

import java.util.List;

/**
 * Author: Felix
 * Date: 2021/11/16
 * Desc: 地区统计Service接口
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(Integer date);
}
