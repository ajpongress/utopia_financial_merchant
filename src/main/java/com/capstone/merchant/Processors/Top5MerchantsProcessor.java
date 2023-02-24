package com.capstone.merchant.Processors;

import com.capstone.merchant.Models.MerchantModel;
import com.capstone.merchant.Models.MerchantTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

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
            // Sort map by value (counter), descending, only return the 5 merchant ids with the highest counter
            Map<MerchantModel, Long> sortedMap = merchantMap.entrySet().stream()
                    .sorted(Entry.comparingByValue(Comparator.reverseOrder()))
                    .limit(5)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        return sortedMap;
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
