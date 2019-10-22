package com.github.flink.dao;

import com.github.flink.domain.ProductEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 9:44 PM
 */
@Mapper
public interface ProductDao {
    ProductEntity selectById(@Param("id") int id);

    List<ProductEntity> selectByIds(@Param("ids") List<String> ids);

    List<String> selectInitPro(@Param("size") int size);
}
