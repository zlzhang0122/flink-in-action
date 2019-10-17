package com.github.flink.scheduler;

import com.github.flink.consts.Constants;
import com.github.flink.domain.ProductPortraitEntity;
import com.github.flink.utils.client.HbaseClient;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * 基于产品
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/17 8:35 PM
 */
public class ProductCoeff {
    private static final Logger logger = Logger.getLogger(ProductCoeff.class);

    public void getSingleProductCoeff(String id, List<String> others) throws Exception {
        ProductPortraitEntity productPortraitEntity = signProduct(id);

        for(String other : others){
            if(id.equalsIgnoreCase(other)){
                continue;
            }

            ProductPortraitEntity item = signProduct(other);
            Double score = getScore(productPortraitEntity, item);
            HbaseClient.putData("ps", id, "p", other, score.toString());
        }
    }

    private ProductPortraitEntity signProduct(String productId){
        ProductPortraitEntity productPortraitEntity = new ProductPortraitEntity();

        try{
            String woman = HbaseClient.getData("prod", productId, "sex", Constants.SEX_WOMAN);
            String man = HbaseClient.getData("prod", productId, "sex", Constants.SEX_MAN);
            String age_10 = HbaseClient.getData("prod", productId, "age", Constants.AGE_10);
            String age_20 = HbaseClient.getData("prod", productId, "age", Constants.AGE_20);
            String age_30 = HbaseClient.getData("prod", productId, "age", Constants.AGE_30);
            String age_40 = HbaseClient.getData("prod", productId, "age", Constants.AGE_40);
            String age_50 = HbaseClient.getData("prod", productId, "age", Constants.AGE_50);
            String age_60 = HbaseClient.getData("prod", productId, "age", Constants.AGE_60);

            productPortraitEntity.setMan(Integer.valueOf(man));
            productPortraitEntity.setWoman(Integer.valueOf(woman));
            productPortraitEntity.setAge_10(Integer.valueOf(age_10));
            productPortraitEntity.setAge_20(Integer.valueOf(age_20));
            productPortraitEntity.setAge_30(Integer.valueOf(age_30));
            productPortraitEntity.setAge_40(Integer.valueOf(age_40));
            productPortraitEntity.setAge_50(Integer.valueOf(age_50));
            productPortraitEntity.setAge_60(Integer.valueOf(age_60));
        }catch (Exception e){
            System.err.println("productId: " + productId);
            e.printStackTrace();
        }

        return productPortraitEntity;
    }

    /**
     * 根据标签计算相关度
     *
     * @param productPortraitEntity
     * @param target
     * @return
     */
    private Double getScore(ProductPortraitEntity productPortraitEntity, ProductPortraitEntity target){
        double sqrt = Math.sqrt(productPortraitEntity.getTotal() + target.getTotal());
        if(sqrt == 0){
            return 0.0;
        }
        int total = productPortraitEntity.getMan() * target.getMan() + productPortraitEntity.getWoman() * target.getWoman()
                + productPortraitEntity.getAge_10() * target.getAge_10() + productPortraitEntity.getAge_20() * target.getAge_20()
                + productPortraitEntity.getAge_30() * target.getAge_30() + productPortraitEntity.getAge_40() * target.getAge_40()
                + productPortraitEntity.getAge_50() * target.getAge_50() + productPortraitEntity.getAge_60() * target.getAge_60();

        return Math.sqrt(total) / sqrt;
    }

    public static void main(String[] args) throws Exception {
        String data = HbaseClient.getData("prod", "2", "sex", "2");
        System.out.println(data);
    }
}
