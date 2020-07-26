package com.tanjunchen.business.model.request;

import java.io.Serializable;

/**
 *
 */
public class NewRecommendationRequest implements Serializable {

    private int sum;

    public NewRecommendationRequest(int sum) {
        this.sum = sum;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
}
