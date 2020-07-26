package com.tanjunchen.business.model.request;

import java.io.Serializable;

/**
 *
 */
public class HotRecommendationRequest implements Serializable {

    private int sum;

    public HotRecommendationRequest(int sum) {
        this.sum = sum;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
}
