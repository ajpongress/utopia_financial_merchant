package com.capstone.merchant.Processors;

import com.capstone.merchant.Models.MerchantModel;
import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@StepScope
@Component
@Slf4j
public class UniqueCountProcessor implements ItemProcessor<MerchantModel, MerchantModel> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    private final Set<Long> merchantIdTracker = new HashSet<>();

    private static long idCounter = 0;

    public long getIdCounter() {
        return idCounter;
    }

    public void clearAllTrackersAndCounters() {
        merchantIdTracker.clear();
        idCounter = 0;
    }

    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    @Override
    public MerchantModel process(MerchantModel merchantModel) {

        synchronized(this) {

            // Avoid duplicate merchants being returned
            if (merchantIdTracker.contains(merchantModel.getMerchantID())) {
                // do nothing
            }

            // Update id counter, add merchant id to tracker
            else {
                idCounter++;
                merchantIdTracker.add(merchantModel.getMerchantID());
            }

            return null;
        }
    }

}
