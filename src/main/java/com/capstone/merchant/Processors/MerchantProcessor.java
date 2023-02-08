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
public class MerchantProcessor implements ItemProcessor<MerchantModel, MerchantModel> {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    private final Set<Long> merchantIdTracker = new HashSet<>();
    private final Faker faker = new Faker();
    private static long idCounter = 0;



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    @Override
    public MerchantModel process(MerchantModel merchantModel) {

        synchronized(this) {

            // Avoid duplicate merchants being returned
            if (merchantIdTracker.contains(merchantModel.getMerchantID())) {
                return null;
            }

            // Set merchant name (non-id)
            merchantModel.setMerchantName(faker.company().name());

            // Set merchant short code
            merchantModel.setMerchantShortCode(faker.number().randomNumber(5, false));

            // Set merchant business category
            merchantModel.setMerchantCategory(faker.company().industry());

            // Update id counter, add merchant id to tracker, print out merchant when processed and return merchant
            idCounter++;
            merchantModel.setId(idCounter);
            merchantIdTracker.add(merchantModel.getMerchantID());
            System.out.println(merchantModel);
            return merchantModel;
        }
    }
}
