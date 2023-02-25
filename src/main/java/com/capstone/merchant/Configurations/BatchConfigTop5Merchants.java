package com.capstone.merchant.Configurations;

import com.capstone.merchant.Controllers.MerchantTransactionController;
import com.capstone.merchant.Listeners.CustomChunkListener;
import com.capstone.merchant.Models.MerchantModel;
import com.capstone.merchant.Processors.Top5MerchantsProcessor;
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
import org.springframework.transaction.PlatformTransactionManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Configuration
@Slf4j
public class BatchConfigTop5Merchants {

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
    private Top5MerchantsProcessor top5MerchantsProcessor;

    @Autowired
    @Qualifier("writer_Merchant")
    private SynchronizedItemStreamWriter<MerchantModel> xmlWriter;

    @Autowired
    @Qualifier("taskExecutor_Merchant")
    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

//    @Autowired
//    private MerchantTransactionClassifier merchantTransactionClassifier;



    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - top 5 recurring merchant transactions
    @Bean
    public Step step_exportTop5Merchants() {

        return new StepBuilder("exportTop5MerchantsStep", jobRepository)
                .<MerchantModel, MerchantModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(top5MerchantsProcessor)
                .writer(xmlWriter)
                .listener(new CustomChunkListener())
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {

                        //merchantTransactionClassifier.closeAllwriters();

                        // Create reports file using reports file path from Controller API call
                        String filePath = MerchantTransactionController.getReportsPath();
                        File top5RecurringReport = new File(filePath);

                        Map<MerchantModel, Long> recurringMerchantMap = top5MerchantsProcessor.getMerchantMap();

                        // Write relevant data to reports file
                        try {
                            BufferedWriter writer = new BufferedWriter(new FileWriter(top5RecurringReport));

                            writer.write("Top 5 Recurring Merchant Transactions");
                            writer.newLine();
                            writer.write("-------------------------------------");
                            writer.newLine();
                            writer.newLine();
                            writer.write("Merchant ID -" + "\t" + "Dollar Amount -" + "\t" + "Recurring Count");
                            writer.newLine();
                            writer.newLine();
                            recurringMerchantMap.forEach((merchant, recurringCount) -> {
                                try {
                                    writer.write(merchant.getMerchantID() + "\t" + merchant.getTransactionAmount() + "\t" + recurringCount);
                                    writer.newLine();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                            writer.close();

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        top5MerchantsProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - top 5 recurring merchant transactions
    @Bean
    public Job job_exportTop5Merchants() {

        return new JobBuilder("exportTop5MerchantsJob", jobRepository)
                .start(step_exportTop5Merchants())
                .build();
    }
}
