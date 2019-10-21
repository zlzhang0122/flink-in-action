package com.github.flink.service.impl;

import com.github.flink.dao.ContactDao;
import com.github.flink.domain.ContactEntity;
import com.github.flink.service.ContactService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/21 7:41 PM
 */
@Service("contactService")
public class ContactServiceImpl implements ContactService {

    @Autowired
    private ContactDao contactDao;

    @Override
    public List<ContactEntity> selectByIds(List<String> ids) {
        return contactDao.selectByIds(ids);
    }

    @Override
    public ContactEntity selectById(String id){
        return contactDao.selectById(Integer.valueOf(id));
    }
}
