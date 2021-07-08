package com.atguigu.bean;

public class UserBehavior {
    private Long UserId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        UserId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public UserBehavior() {
    }

    public void setUserId(Long userId) {
        UserId = userId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return UserId;
    }

    public Long getItemId() {
        return itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "UserId=" + UserId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
