package com.capstone.merchant.Configurations;

import com.capstone.merchant.Classifiers.MerchantTransactionClassifier;
import com.capstone.merchant.Models.MerchantTransactionModel;
import com.capstone.merchant.Processors.SingleMerchantProcessor;
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
public class BatchConfigSingleMerchant {

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
    private SingleMerchantProcessor singleMerchantProcessor;

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

    // Step - single merchant transactions
    @Bean
    public Step step_exportSingleMerchant() {

        return new StepBuilder("exportSingleMerchantStep", jobRepository)
                .<MerchantTransactionModel, MerchantTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(singleMerchantProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        merchantTransactionClassifier.closeAllwriters();
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        singleMerchantProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - single merchant transactions
    @Bean
    public Job job_exportSingleMerchant() {

        return new JobBuilder("exportSingleMerchantJob", jobRepository)
                .start(step_exportSingleMerchant())
                .build();
    }
}
