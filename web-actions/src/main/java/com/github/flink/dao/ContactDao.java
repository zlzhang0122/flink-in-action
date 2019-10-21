package com.github.flink.dao;

import com.github.flink.domain.ContactEntity;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.data.repository.query.Param;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/21 7:42 PM
 */
@Mapper
public interface ContactDao {

    ContactEntity selectById(@Param("id") int id);

    List<ContactEntity> selectByIds(@Param("ids") List<String> ids);
}
