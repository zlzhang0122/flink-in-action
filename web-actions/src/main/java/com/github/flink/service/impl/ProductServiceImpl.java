package com.github.flink.service.impl;

import com.github.flink.dao.ProductDao;
import com.github.flink.domain.ProductEntity;
import com.github.flink.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 9:41 PM
 */
@Service
public class ProductServiceImpl implements ProductService {

    @Autowired
    private ProductDao productDao;

    @Override
    public ProductEntity selectById(String id) {
        return productDao.selectById(Integer.valueOf(id));
    }

    @Override
    public List<ProductEntity> selectByIds(List<String> ids) {
        return productDao.selectByIds(ids);
    }

    @Override
    public List<String> selectInitPro(int topSize) {
        return productDao.selectInitPro(topSize);
    }
}
