package com.atguigu.bean;

public class PageViewCount {
    private String PV;
    private String Time;
    private Long PvCount;

    public String getPV() {
        return PV;
    }

    public String getTime() {
        return Time;
    }

    public Long getPvCount() {
        return PvCount;
    }

    public PageViewCount() {
    }

    public PageViewCount(String PV, String time, Long pvCount) {
        this.PV = PV;
        Time = time;
        PvCount = pvCount;
    }

    public void setPV(String PV) {
        this.PV = PV;
    }

    public void setTime(String time) {
        Time = time;
    }

    public void setPvCount(Long pvCount) {
        PvCount = pvCount;
    }

    @Override
    public String toString() {
        return "PageViewCount{" +
                "PV='" + PV + '\'' +
                ", Time='" + Time + '\'' +
                ", PvCount=" + PvCount +
                '}';
    }
}
