package com.capstone.merchant.Processors;

import com.capstone.merchant.Models.MerchantModel;
import com.capstone.merchant.Models.MerchantTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class Top5MerchantsProcessor implements ItemProcessor<MerchantModel, MerchantModel> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    // Tracks merchant transactions
    // Primary key = Model (contains merchant ID and dollar amount)
    // Value = counter counting how many times dollar amount was seen
    HashMap<MerchantModel, Long> merchantMap = new HashMap<>();

    public Map<Long, List<Map.Entry<MerchantModel, Long>>> getMerchantMap() {

        HashMap<MerchantModel, Long> tempMap = new HashMap<>();
        long tempMerchantID = 0L;

        // Sort hashmap by top 5 recurring dollar amounts
        merchantMap = merchantMap
                .entrySet().stream()
                .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                //.limit(5)
                .map(Map.Entry::getKey)
                .collect(HashMap::new, (map, key) -> map.put(key, merchantMap.get(key)), Map::putAll);

//        for (MerchantModel m : merchantMap.keySet()) {
//
//            if (m.getMerchantID() == tempMerchantID) {
//                // do nothing
//            }
//            else {
//                tempMap.put(m, merchantMap.get(m));
//            }
//
//            tempMerchantID = m.getMerchantID();
//
//        }



        return merchantMap.entrySet().stream()
                .collect(Collectors.groupingBy(
                        x -> x.getKey().getMerchantID()
                ));
    }

    private long tempCounter = 0L;

    public void clearAllTrackersAndCounters() {
        merchantMap.clear();
    }

    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    public MerchantModel process(MerchantModel transaction) {

        synchronized (this) {

            // Strip negative sign from MerchantID
            long temp_MerchantID = Math.abs(transaction.getMerchantID());
            transaction.setMerchantID(temp_MerchantID);

            // New merchant transaction with dollar amount
            if (!merchantMap.containsKey(transaction)) {

                merchantMap.put(transaction, 1L);
            }

            // Merchant transaction (with dollar amount) has already been seen
            else {

                // Get counter and increment it
                tempCounter = merchantMap.get(transaction);
                tempCounter++;
                merchantMap.replace(transaction, tempCounter);

            }

            return null; // don't return to writer. Only a report will get generated at end of step
        }
    }

}
