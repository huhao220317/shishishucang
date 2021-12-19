package com.atguigu.gmall.service;

import com.atguigu.gmall.beans.ProductStats;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsService {
    public BigDecimal getGMV(Integer date);
    List<ProductStats> getProductStatsByTm(Integer date,Integer limite);

    List<ProductStats> getProductStatsByCategory3(Integer date,Integer limit);

    List<ProductStats> getProductStatsBySpu(Integer date,Integer limit);

}
