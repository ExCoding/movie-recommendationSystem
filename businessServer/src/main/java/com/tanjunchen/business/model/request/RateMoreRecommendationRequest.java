package com.tanjunchen.business.model.request;

import java.io.Serializable;

/**
 *
 */
public class RateMoreRecommendationRequest implements Serializable {

    private int sum;

    public RateMoreRecommendationRequest(int sum) {
        this.sum = sum;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
}
