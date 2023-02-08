package com.capstone.merchant.Writers;

import com.capstone.merchant.Classifiers.MerchantTransactionClassifier;
import com.capstone.merchant.Models.MerchantTransactionModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MerchantTransactionCompositeWriter {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    MerchantTransactionClassifier merchantTransactionClassifier;



    // ----------------------------------------------------------------------------------
    // --                                METHODS                                       --
    // ----------------------------------------------------------------------------------

    @Bean("writer_MerchantTransaction")
    public ClassifierCompositeItemWriter<MerchantTransactionModel> classifierCompositeItemWriter() {

        ClassifierCompositeItemWriter<MerchantTransactionModel> writer = new ClassifierCompositeItemWriter<>();
        writer.setClassifier(merchantTransactionClassifier);

        return writer;
    }
}
