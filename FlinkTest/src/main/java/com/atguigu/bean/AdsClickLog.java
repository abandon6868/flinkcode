package com.atguigu.bean;

public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;

    public AdsClickLog(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public Long getAdId() {
        return adId;
    }

    public String getProvince() {
        return province;
    }

    public String getCity() {
        return city;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public AdsClickLog() {
    }
}
