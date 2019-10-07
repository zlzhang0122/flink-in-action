package com.github.flink.rdbms.commons;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 18:14
 */
public class CommonBean {
    public CommonBean(){
    }

    public CommonBean(Long userId, Long itemId, Long categoryId, String behavior, String ts){
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.ts = ts;
    }

    private Long userId;

    private Long itemId;

    private Long categoryId;

    private String behavior;

    private String ts;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "CommonBean{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", ts='" + ts + '\'' +
                '}';
    }
}
