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

    public Map<MerchantModel, Long> getMerchantMap() {

            // Sort map by value (counter)
            Map<Long, List<Map.Entry<MerchantModel, Long>>> sortedMap = merchantMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.groupingBy(Map.Entry::getValue));

            // Only keep one MerchantModel per MerchantId
            for (Map.Entry<Long, List<Map.Entry<MerchantModel, Long>>> entry : sortedMap.entrySet()) {
                List<Map.Entry<MerchantModel, Long>> list = entry.getValue();
                for (int i = 1; i < list.size(); i++) {
                    list.remove(i);
                }
            }

            // Only keep top 5
            int counter = 0;
            for (Map.Entry<Long, List<Map.Entry<MerchantModel, Long>>> entry : sortedMap.entrySet()) {
                if (counter < 5) {
                    counter++;
                } else {
                    sortedMap.remove(entry.getKey());
                }
            }

            // Convert back to HashMap
            HashMap<MerchantModel, Long> tempMap = new HashMap<>();
            for (Map.Entry<Long, List<Map.Entry<MerchantModel, Long>>> entry : sortedMap.entrySet()) {
                List<Map.Entry<MerchantModel, Long>> list = entry.getValue();
                for (Map.Entry<MerchantModel, Long> entry2 : list) {
                    tempMap.put(entry2.getKey(), entry2.getValue());
                }
            }
            return tempMap;
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
