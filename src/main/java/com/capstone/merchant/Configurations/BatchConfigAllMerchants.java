package com.capstone.merchant.Configurations;

import com.capstone.merchant.Classifiers.MerchantTransactionClassifier;
import com.capstone.merchant.Models.MerchantTransactionModel;
import com.capstone.merchant.Processors.AllMerchantsProcessor;
import com.capstone.merchant.TaskExecutors.TaskExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Slf4j
public class BatchConfigAllMerchants {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("reader_MerchantTransaction")
    private SynchronizedItemStreamReader<MerchantTransactionModel> synchronizedItemStreamReader;

    @Autowired
    private AllMerchantsProcessor allMerchantsProcessor;

    @Autowired
    @Qualifier("writer_MerchantTransaction")
    private ClassifierCompositeItemWriter<MerchantTransactionModel> classifierCompositeItemWriter;

    @Autowired
    @Qualifier("taskExecutor_Merchant")
    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

    @Autowired
    private MerchantTransactionClassifier merchantTransactionClassifier;



    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - all merchants transactions
    @Bean
    public Step step_exportAllMerchants() {

        return new StepBuilder("exportAllMerchantsStep", jobRepository)
                .<MerchantTransactionModel, MerchantTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(allMerchantsProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        merchantTransactionClassifier.closeAllwriters();
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - all merchants transactions
    @Bean
    public Job job_exportAllMerchants() {

        return new JobBuilder("exportAllMerchantsJob", jobRepository)
                .start(step_exportAllMerchants())
                .build();
    }
}
