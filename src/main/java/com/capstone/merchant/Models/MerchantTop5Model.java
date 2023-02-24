package com.capstone.merchant.Models;

import lombok.Data;

@Data
public class MerchantTop5Model {

    private long merchantID; // CSV - Merchant Name (index 8) ----- STRIP NEGATIVE SIGN

    private String transactionAmount; // CSV - Amount (index 6)
}
