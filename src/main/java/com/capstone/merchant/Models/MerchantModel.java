package com.capstone.merchant.Models;

import lombok.Data;

@Data
public class MerchantModel {

    private long id;
    private long merchantID;
    private String transactionAmount; // CSV - Amount (index 6)
    private String merchantName;
    private long merchantShortCode;
    private String merchantCategory;
}
