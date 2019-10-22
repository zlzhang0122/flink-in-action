package com.github.flink.domain;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:45 PM
 */
public class ProductScoreEntity {
    private ProductEntity productEntity;

    private double score;

    private int rank;

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

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return "ProductScoreEntity{" +
                "productEntity=" + productEntity +
                ", score=" + score +
                ", rank=" + rank +
                '}';
    }
}
