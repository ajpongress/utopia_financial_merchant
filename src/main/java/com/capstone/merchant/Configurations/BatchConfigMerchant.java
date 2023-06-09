package com.capstone.merchant.Configurations;

import com.capstone.merchant.Controllers.MerchantController;
import com.capstone.merchant.Models.MerchantModel;
import com.capstone.merchant.Processors.MerchantProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Configuration
@Slf4j
public class BatchConfigMerchant {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("reader_Merchant")
    private SynchronizedItemStreamReader<MerchantModel> synchronizedItemStreamReader;

    @Autowired
    private MerchantProcessor merchantProcessor;

    @Autowired
    @Qualifier("writer_Merchant")
    private SynchronizedItemStreamWriter<MerchantModel> xmlWriter;

    @Autowired
    private TaskExecutor taskExecutor;



    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - merchant generation
    @Bean
    public Step step_generateMerchants() {

        return new StepBuilder("generateMerchantsStep", jobRepository)
                .<MerchantModel, MerchantModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(merchantProcessor)
                .writer(xmlWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {

//                        // Create reports file using reports file path from Controller API call
//                        String filePath = MerchantController.getReportsPath();
//                        File uniqueMerchantsReport = new File(filePath);
//
//                        // Write relevant data to reports file
//                        try {
//                            BufferedWriter writer = new BufferedWriter(new FileWriter(uniqueMerchantsReport));
//                            writer.write("Total unique merchants = " + stepExecution.getWriteCount());
//                            writer.close();
//
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//
//                        log.info("------------------------------------------------------------------");
//                        log.info("Total unique merchants = " + stepExecution.getWriteCount());
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        merchantProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(taskExecutor)
                .build();
    }

    // Job - merchant generation
    @Bean
    public Job job_generateMerchants() {

        return new JobBuilder("generateMerchantsJob", jobRepository)
                .start(step_generateMerchants())
                .build();
    }
}
