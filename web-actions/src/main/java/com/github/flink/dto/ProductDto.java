package com.github.flink.dto;

import com.github.flink.domain.ContactEntity;
import com.github.flink.domain.ProductEntity;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:49 PM
 */
public class ProductDto {

    private ContactEntity contactEntity;

    private ProductEntity productEntity;

    private double score;

    public ContactEntity getContactEntity() {
        return contactEntity;
    }

    public void setContactEntity(ContactEntity contactEntity) {
        this.contactEntity = contactEntity;
    }

    public ProductEntity getProductEntity() {
        return productEntity;
    }

    public void setProductEntity(ProductEntity productEntity) {
        this.productEntity = productEntity;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "ProductDto{" +
                "contactEntity=" + contactEntity +
                ", productEntity=" + productEntity +
                ", score=" + score +
                '}';
    }
}
