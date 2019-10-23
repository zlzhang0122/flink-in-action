package com.github.flink.service.impl;

import com.github.flink.client.RedisClient;
import com.github.flink.domain.ContactEntity;
import com.github.flink.domain.ProductEntity;
import com.github.flink.domain.ProductScoreEntity;
import com.github.flink.dto.ProductDto;
import com.github.flink.service.ContactService;
import com.github.flink.service.ProductService;
import com.github.flink.service.RecommandService;
import com.github.flink.service.UserScoreService;
import com.github.flink.utils.client.HbaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:53 PM
 */
@Service("recommandService")
public class RecommandServiceImpl implements RecommandService {
    private final Logger logger = LoggerFactory.getLogger(RecommandService.class);

    @Autowired
    private UserScoreService userScoreService;

    @Autowired
    private ProductService productService;

    @Autowired
    private ContactService contactService;

    @Resource
    private RedisClient redisClient;

    private final static int TOP_SIZE = 10;

    private final static int PRODUCT_LIMIT = 3;

    @Override
    public List<ProductScoreEntity> userRecommand(String userId) throws IOException {
        //排序
        List<ProductScoreEntity> productScoreEntityList = userScoreService.getTopRankProduct(userId);
        productScoreEntityList.sort((a, b) -> {
            Double compare = a.getScore() - b.getScore();
            if(compare > 0){
                return -1;
            }else{
                return 1;
            }
        });

        List<ProductScoreEntity> ret = new ArrayList<>();
        productScoreEntityList.forEach(r -> {
            try{
                ret.add(r);
                List<Map.Entry> ps = HbaseClient.getRow("ps", userId);
                int end = ps.size() > 3 ? ps.size() : 3;
                for(int i = 0; i< end; i++){
                    Map.Entry entry = ps.get(i);
                    ProductEntity productEntity = productService.selectById((String)entry.getKey());
                    ProductScoreEntity productScoreEntity = new ProductScoreEntity();
                    productScoreEntity.setProductEntity(productEntity);
                    productScoreEntity.setScore(r.getScore());
                    productScoreEntity.setRank(r.getRank());

                    ret.add(productScoreEntity);
                }
            }catch (Exception e){
                logger.error("userRecommand error", e);
            }
        });

        return ret;
    }

    /**
     * 用默认的热门商品
     *
     * @return
     */
    @Override
    public List<ProductDto> recommandByHotList() {
        //top榜单
        List<String> topList = getDefaultTop();
        int topSize = topList.size();

        //商品详情表
        List<ContactEntity> contactEntityList = contactService.selectByIds(topList);
        //商标基本信息表
        List<ProductEntity> productEntityList = productService.selectByIds(topList);
        return fillProductDto(topList, contactEntityList, productEntityList, topSize);
    }

    /**
     * 协同过滤算法
     *
     * @return
     * @throws IOException
     */
    @Override
    public List<ProductDto> recommandByItemCfCoeff() throws IOException {
        return recommandByItemCfCoeff("px");
    }

    /**
     * 余弦相似度算法
     *
     * @return
     * @throws IOException
     */
    @Override
    public List<ProductDto> recommandByProductCoeff() throws IOException {
        return recommandByItemCfCoeff("ps");
    }

    /**
     * 选择算法获取数据
     *
     * @return
     * @throws IOException
     */
    private List<ProductDto> recommandByItemCfCoeff(String algorithm) throws IOException {
        List<String> topList = getDefaultTop();
        List<String> ps = addRecommandProduct(topList, algorithm);
        ps = removeDuplicateWithOrder(ps);

        //商品详情
        List<ContactEntity> contactEntityList = contactService.selectByIds(ps);
        //商品基本信息
        List<ProductEntity> productEntityList = productService.selectByIds(ps);
        return transferToDto(ps, contactEntityList, productEntityList);
    }

    private List<String> addRecommandProduct(List<String> topList, String table){
        List<String> ret = new ArrayList<>();
        for(String s : topList){
            ret.add(s);

            List<Map.Entry> ps = new ArrayList<>();

            try{
                ps = HbaseClient.getRow(table, s);
                Collections.sort(ps, ((o1, o2) -> -(new BigDecimal(o1.getValue().toString()).compareTo(
                        new BigDecimal(o2.getValue().toString())
                ))));
            }catch (Exception e){
                logger.warn("Hbase中没有产品[{}]", s);
                logger.error("addRecommandProduct error", e);
            }

            if(CollectionUtils.isEmpty(ps)){
                continue;
            }

            int end = Math.min(ps.size(), PRODUCT_LIMIT);
            for(int i = 0; i< end; i++){
                ret.add((String)ps.get(i).getKey());
            }
        }

        return ret;
    }

    /**
     * 结果数量用数量最小的值
     *
     * @param list
     * @param contactEntityList
     * @param productEntityList
     * @return
     */
    private List<ProductDto> transferToDto(List<String> list, List<ContactEntity> contactEntityList,
                                           List<ProductEntity> productEntityList){
        int topSize = Math.min(Math.min(list.size(), contactEntityList.size()), productEntityList.size());
        return fillProductDto(list, contactEntityList, productEntityList, topSize);
    }

    /**
     * 将商品详情和基本信息结合转换为商品dto
     *
     * @param list
     * @param contactEntityList
     * @param productEntityList
     * @param topSize
     * @return
     */
    private List<ProductDto> fillProductDto(List<String> list, List<ContactEntity> contactEntityList,
                                            List<ProductEntity> productEntityList, int topSize){
        List<ProductDto> productDtoList = new ArrayList<>();
        for(int i = 0; i < topSize; i++){
            String topId = list.get(i);
            ProductDto productDto = new ProductDto();
            productDto.setScore(TOP_SIZE + 1 - i);
            for(int j = 0; j < topSize; j++){
                if(topId.equalsIgnoreCase(String.valueOf(contactEntityList.get(j).getId()))){
                    productDto.setContactEntity(contactEntityList.get(j));
                }
                if(topId.equalsIgnoreCase(String.valueOf(productEntityList.get(j).getProductId()))){
                    productDto.setProductEntity(productEntityList.get(j));
                }
            }

            productDtoList.add(productDto);
        }

        return productDtoList;
    }

    /**
     * 去重
     *
     * @param list
     * @param <T>
     * @return
     */
    public static <T> List<T> removeDuplicateWithOrder(List<T> list){
        return list.stream().distinct().collect(Collectors.toList());
    }

    private List<String> getDefaultTop(){
        List<String> topList = redisClient.getTopList(TOP_SIZE);
        topList = topList.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if(topList.size() < 10){
            topList.addAll(productService.selectInitPro(100));
            topList = topList.stream().distinct().collect(Collectors.toList());

            logger.info("top: {}", topList);
        }

        return topList;
    }
}
