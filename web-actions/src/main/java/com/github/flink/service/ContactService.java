package com.github.flink.service;

import com.github.flink.domain.ContactEntity;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/21 7:37 PM
 */
public interface ContactService {
    List<ContactEntity> selectByIds(List<String> ids);

    ContactEntity selectById(String id);
}
