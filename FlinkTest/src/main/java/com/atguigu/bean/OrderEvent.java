package com.atguigu.bean;

public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String eventType, String txId, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.eventTime = eventTime;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getOrderId() {
        return orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTxId() {
        return txId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
