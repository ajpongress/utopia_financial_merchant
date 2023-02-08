package com.capstone.merchant.Processors;


import com.capstone.merchant.Models.MerchantTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component
@Slf4j
public class AllMerchantsProcessor implements ItemProcessor<MerchantTransactionModel, MerchantTransactionModel> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    private final HashMap<Long, String> merchantTransactionModelMap = new HashMap<>();

    // Useful for additional jobs or steps
    public void clearMap() {
        merchantTransactionModelMap.clear();
    }

    private static long transactionIdCounter = 0;

    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    public MerchantTransactionModel process(MerchantTransactionModel merchantTransactionModel) {

        synchronized (this) {

            // Strip negative sign from MerchantID
            long temp_MerchantID = Math.abs(merchantTransactionModel.getMerchantID());
            merchantTransactionModel.setMerchantID(temp_MerchantID);

            // Strip fractional part of MerchantTransactionModelZip if greater than 5 characters
            if (merchantTransactionModel.getTransactionZip().length() > 5) {
                String[] temp_MerchantTransactionModelZip = merchantTransactionModel.getTransactionZip().split("\\.", 0);
                merchantTransactionModel.setTransactionZip(temp_MerchantTransactionModelZip[0]);
            }

            // Print processed merchantTransactionModel and return
            transactionIdCounter++;
            merchantTransactionModel.setId(transactionIdCounter);
            log.info(merchantTransactionModel.toString());
            return merchantTransactionModel;

        }
    }
}
