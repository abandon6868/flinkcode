package com.atguigu.bean;

public class AvgVC {
    private Long vcSum;
    private Long vcCount;

    public void setVcSum(Long vcSum) {
        this.vcSum = vcSum;
    }

    public void setVcCount(Long vcCount) {
        this.vcCount = vcCount;
    }

    public Long getVcSum() {
        return vcSum;
    }

    public Long getVcCount() {
        return vcCount;
    }

    public AvgVC() {
    }

    public AvgVC(Long vcSum, Long vcCount) {
        this.vcSum = vcSum;
        this.vcCount = vcCount;
    }
}
