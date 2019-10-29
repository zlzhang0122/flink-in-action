package com.github.flink.networkanalysis.model;

/**
 * url访问统计
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/29 6:10 PM
 */
public class UrlViewCount {

    public UrlViewCount(){

    }

    public UrlViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    private String url;

    private Long windowEnd;

    private Long count;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UserViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
