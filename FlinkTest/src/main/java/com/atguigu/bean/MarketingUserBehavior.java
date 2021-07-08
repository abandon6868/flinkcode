package com.atguigu.bean;

public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;

    public MarketingUserBehavior() {
    }

    public MarketingUserBehavior(Long userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public String getChannel() {
        return channel;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
