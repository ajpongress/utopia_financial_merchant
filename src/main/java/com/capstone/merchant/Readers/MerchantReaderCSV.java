package com.capstone.merchant.Readers;

import com.capstone.merchant.Models.MerchantModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MerchantReaderCSV {

    // FlatFileItemReader
    @StepScope
    @Bean("reader_Merchant")
    public SynchronizedItemStreamReader<MerchantModel> synchronizedItemStreamReader(
            @Value("#{jobParameters['file.input']}")
            String source_input
    ) throws UnexpectedInputException,
            NonTransientResourceException, ParseException {

        FlatFileItemReader<MerchantModel> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(source_input));
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper((line, lineNumber) -> {
            String[] fields = line.split(",");
            MerchantModel merchantModel = new MerchantModel();

            merchantModel.setMerchantID(Math.abs(Long.parseLong(fields[8])));
            merchantModel.setTransactionAmount(fields[6]);

            return merchantModel;
        });

        // Make FlatFileItemReader thread-safe
        return new SynchronizedItemStreamReaderBuilder<MerchantModel>()
                .delegate(itemReader)
                .build();
    }
}
