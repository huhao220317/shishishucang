package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.beans.ProductStats;
import com.atguigu.gmall.mapper.ProductStatsMapper;
import com.atguigu.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return productStatsMapper.selectGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsByTm(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByTm(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsByCategory3(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsByCategory3(date,limit);
    }

    @Override
    public List<ProductStats> getProductStatsBySpu(Integer date, Integer limit) {
        return productStatsMapper.selectProductStatsBySpu(date,limit);
    }
}
