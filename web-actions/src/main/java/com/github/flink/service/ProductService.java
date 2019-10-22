package com.github.flink.service;

import com.github.flink.domain.ProductEntity;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 9:40 PM
 */
public interface ProductService {

    ProductEntity selectById(String id);

    List<ProductEntity> selectByIds(List<String> ids);

    List<String> selectInitPro(int topSize);
}
